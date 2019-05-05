package com.aliyun.openservices.shade.com.alibaba.fastjson.util;

import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.openservices.shade.com.alibaba.fastjson.JSON;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONArray;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONAware;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONException;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONPath;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONPathException;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONReader;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONStreamAware;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSONWriter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.TypeReference;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.DefaultJSONParser;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.Feature;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.JSONLexer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.JSONLexerBase;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.JSONReaderScanner;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.JSONScanner;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.JSONToken;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.ParseContext;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.ParserConfig;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.SymbolTable;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.deserializer.AutowiredObjectDeserializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.deserializer.DefaultFieldDeserializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.deserializer.ExtraProcessable;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.deserializer.ExtraProcessor;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.deserializer.ExtraTypeProvider;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.deserializer.FieldDeserializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.deserializer.JavaBeanDeserializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.AfterFilter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.BeanContext;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.BeforeFilter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.ContextObjectSerializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.ContextValueFilter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.JSONSerializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.JavaBeanSerializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.LabelFilter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.Labels;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.NameFilter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.ObjectSerializer;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.PropertyFilter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.PropertyPreFilter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.SerialContext;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.SerializeBeanInfo;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.SerializeConfig;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.SerializeFilter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.SerializeFilterable;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.SerializeWriter;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.SerializerFeature;
import com.aliyun.openservices.shade.com.alibaba.fastjson.serializer.ValueFilter;

public class ASMClassLoader extends ClassLoader {

    private static java.security.ProtectionDomain DOMAIN;
    
    private static Map<String, Class<?>> classMapping = new HashMap<String, Class<?>>();

    static {
        DOMAIN = (java.security.ProtectionDomain) java.security.AccessController.doPrivileged(new PrivilegedAction<Object>() {

            public Object run() {
                return ASMClassLoader.class.getProtectionDomain();
            }
        });
        
        Class<?>[] jsonClasses = new Class<?>[] {JSON.class,
            JSONObject.class,
            JSONArray.class,
            JSONPath.class,
            JSONAware.class,
            JSONException.class,
            JSONPathException.class,
            JSONReader.class,
            JSONStreamAware.class,
            JSONWriter.class,
            TypeReference.class,
                    
            FieldInfo.class,
            TypeUtils.class,
            IOUtils.class,
            IdentityHashMap.class,
            ParameterizedTypeImpl.class,
            JavaBeanInfo.class,
                    
            ObjectSerializer.class,
            JavaBeanSerializer.class,
            SerializeFilterable.class,
            SerializeBeanInfo.class,
            JSONSerializer.class,
            SerializeWriter.class,
            SerializeFilter.class,
            Labels.class,
            LabelFilter.class,
            ContextValueFilter.class,
            AfterFilter.class,
            BeforeFilter.class,
            NameFilter.class,
            PropertyFilter.class,
            PropertyPreFilter.class,
            ValueFilter.class,
            SerializerFeature.class,
            ContextObjectSerializer.class,
            SerialContext.class,
            SerializeConfig.class,
                    
            JavaBeanDeserializer.class,
            ParserConfig.class,
            DefaultJSONParser.class,
            JSONLexer.class,
            JSONLexerBase.class,
            ParseContext.class,
            JSONToken.class,
            SymbolTable.class,
            Feature.class,
            JSONScanner.class,
            JSONReaderScanner.class,
                    
            AutowiredObjectDeserializer.class,
            ObjectDeserializer.class,
            ExtraProcessor.class,
            ExtraProcessable.class,
            ExtraTypeProvider.class,
            BeanContext.class,
            FieldDeserializer.class,
            DefaultFieldDeserializer.class,
        };
        
        for (Class<?> clazz : jsonClasses) {
            classMapping.put(clazz.getName(), clazz);
        }
    }
    
    public ASMClassLoader(){
        super(getParentClassLoader());
    }

    public ASMClassLoader(ClassLoader parent){
        super (parent);
    }

    static ClassLoader getParentClassLoader() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader != null) {
            try {
                contextClassLoader.loadClass(JSON.class.getName());
                return contextClassLoader;
            } catch (ClassNotFoundException e) {
                // skip
            }
        }
        return JSON.class.getClassLoader();
    }

    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> mappingClass = classMapping.get(name);
        if (mappingClass != null) {
            return mappingClass;
        }
        
        try {
            return super.loadClass(name, resolve);
        } catch (ClassNotFoundException e) {
            throw e;
        }
    }

    public Class<?> defineClassPublic(String name, byte[] b, int off, int len) throws ClassFormatError {
        Class<?> clazz = defineClass(name, b, off, len, DOMAIN);

        return clazz;
    }

    public boolean isExternalClass(Class<?> clazz) {
        ClassLoader classLoader = clazz.getClassLoader();

        if (classLoader == null) {
            return false;
        }

        ClassLoader current = this;
        while (current != null) {
            if (current == classLoader) {
                return false;
            }

            current = current.getParent();
        }

        return true;
    }

}
