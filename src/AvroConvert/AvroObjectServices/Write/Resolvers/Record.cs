#region license
/**Copyright (c) 2022 Adrian Strugala
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* https://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#endregion

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using SolTechnology.Avro.Features.Serialize;
using SolTechnology.Avro.AvroObjectServices.Schemas;
using SolTechnology.Avro.AvroObjectServices.Write.Resolvers;
using static SolTechnology.Avro.Features.Serialize.Encoder;
using static SolTechnology.Avro.AvroObjectServices.Write.WriteResolver;

// ReSharper disable once CheckNamespace
namespace SolTechnology.Avro.AvroObjectServices.Write
{
    internal partial class WriteResolver
    {
        private static readonly ConcurrentDictionary<Type, Lazy<Action<object, IWriter>>> writersDictionary = new();

        internal Encoder.WriteItem ResolveRecord(RecordSchema recordSchema)
        {
            WriteStep[] writeSteps = new WriteStep[recordSchema.Fields.Count];

            int index = 0;
            foreach (RecordFieldSchema field in recordSchema.Fields)
            {
                var record = new WriteStep
                {
                    WriteField = ResolveWriter(field.TypeSchema),
                    FieldName = field.Aliases.FirstOrDefault() ?? field.Name,
                };
                writeSteps[index++] = record;
            }
            
            return RecordResolver;

            void RecordResolver(object v, IWriter e)
            {
                WriteRecordFields(v, writeSteps, e);
            }
        }

        private static void WriteRecordFields(object recordObj, WriteStep[] writers, IWriter encoder)
        {
            if (recordObj is null)
            {
                encoder.WriteNull();
                return;
            }

            if (recordObj is ExpandoObject expando)
            {
                HandleExpando(writers, encoder, expando);
                return;
            }

            var type = recordObj.GetType();

#if NET6_0_OR_GREATER
            var lazyWriters = writersDictionary.GetOrAdd(type, Factory, writers);
#else
            var lazyWriters = writersDictionary.GetOrAdd(type, t => new Lazy<Action<object, IWriter>>(() => GetRecordWriter(t, writers)));
#endif
            Action<object, IWriter> recordWriter = lazyWriters.Value;

            recordWriter.Invoke(recordObj, encoder);
        }

        private static Func<Type, WriteStep[], Lazy<Action<object, IWriter>>> Factory =>
            (type, writeSteps) => new Lazy<Action<object, IWriter>>(() => GetRecordWriter(type, writeSteps),
                LazyThreadSafetyMode.ExecutionAndPublication);

        private static Func<Type, Lazy<Action<object, IWriter>>> Factory2 =>
            (type) => new Lazy<Action<object, IWriter>>(() => GetRecordWriter(type, Array.Empty<WriteStep>()),
                LazyThreadSafetyMode.ExecutionAndPublication);

        private static void HandleExpando(WriteStep[] writers, IWriter encoder, ExpandoObject expando)
        {
            var expandoDictionary = expando.ToDictionary(x => x.Key, y => y.Value, StringComparer.InvariantCultureIgnoreCase);

            foreach (var writer in writers)
            {
                expandoDictionary.TryGetValue(writer.FieldName, out var value);
                writer.WriteField(value, encoder);
            }
        }

        private static Action<object, IWriter> GetRecordWriter(Type type, WriteStep[] writeSteps)
        {
            var namePropertyInfoMap =
                type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase |
                                   BindingFlags.FlattenHierarchy)
                    .ToDictionary(pi => pi.Name, pi => pi);

            var instance = Expression.Parameter(typeof(object), "instance");
            var writer = Expression.Parameter(typeof(IWriter), "writer");

            var actualInstance = Expression.Variable(type, "actualInstance");

            var expressions = new List<Expression>
            {
                Expression.Assign(actualInstance, Expression.Convert(instance, type))
            };

            for (var index = 0; index < writeSteps.Length; index++)
            {
                var writeStep = writeSteps[index];

                if (namePropertyInfoMap.TryGetValue(writeStep.FieldName, out var propInfo))
                {
                    var propertyAccess = Expression.Property(actualInstance, propInfo);
                    if (propInfo.PropertyType.IsValueType)
                    {
                        var methodCallExpression = GetMethodCall(propInfo.PropertyType, writer, propertyAccess);
                        expressions.Add(methodCallExpression);
                    }
                    else
                    {
                        // Convert the property value to object, as WriteItem expects an object as the first parameter.
                        var convertedPropertyAccess = Expression.Convert(propertyAccess, typeof(object));

                        // Create the delegate invocation expression.
                        var writeFieldDelegate = Expression.Constant(writeStep.WriteField, typeof(WriteItem));
                        var delegateInvokeExpression = Expression.Invoke(writeFieldDelegate, convertedPropertyAccess, writer);

                        expressions.Add(delegateInvokeExpression);
                    }
                }
            }

            var block = Expression.Block(new[] { actualInstance }, expressions);
            return Expression.Lambda<Action<object, IWriter>>(block, instance, writer).Compile();

            static MethodCallExpression GetMethodCall(Type primitiveType, ParameterExpression writer, Expression propertyAccess)
            {
                if (primitiveType == typeof(int))
                    return Expression.Call(writer, nameof(IWriter.WriteInt), Type.EmptyTypes, propertyAccess);
                if (primitiveType == typeof(Guid))
                    return Expression.Call(writer, nameof(IWriter.WriteGuid), Type.EmptyTypes, propertyAccess);
                if (primitiveType == typeof(bool))
                    return Expression.Call(writer, nameof(IWriter.WriteBoolean), Type.EmptyTypes, propertyAccess);
                if (primitiveType == typeof(long))
                    return Expression.Call(writer, nameof(IWriter.WriteLong), Type.EmptyTypes, propertyAccess);
                if (primitiveType.IsEnum)
                {
                    var enumAsInt = Expression.Convert(propertyAccess, typeof(int));
                    return Expression.Call(writer, nameof(IWriter.WriteInt), Type.EmptyTypes, enumAsInt);
                }

                throw new NotImplementedException();
            }
        }
    }
}