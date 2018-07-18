package org.talend.components.netsuite.runtime.json;

import java.util.Collection;

import javax.xml.datatype.XMLGregorianCalendar;

import org.talend.components.netsuite.runtime.model.BasicMetaData;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;

/**
 *
 */
public class NsTypeResolverBuilder extends ObjectMapper.DefaultTypeResolverBuilder {

    public static final String TYPE_PROPERTY_NAME = "nsType";

    private BasicMetaData basicMetaData;

    public NsTypeResolverBuilder(BasicMetaData basicMetaData) {
        super(ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE);

        this.basicMetaData = basicMetaData;

        init(JsonTypeInfo.Id.NAME, null);
        inclusion(JsonTypeInfo.As.PROPERTY);
        typeProperty(TYPE_PROPERTY_NAME);
    }

    @Override
    public boolean useForType(JavaType t) {
        if (t.isCollectionLikeType()) {
            return false;
        }
        if (t.getRawClass() == XMLGregorianCalendar.class) {
            return false;
        }
        return super.useForType(t);
    }

    @Override
    protected TypeIdResolver idResolver(MapperConfig<?> config, JavaType baseType, Collection<NamedType> subtypes, boolean forSer,
            boolean forDeser) {

        if (_idType == null) {
            throw new IllegalStateException("Can not build, 'init()' not yet called");
        }

        return new NsTypeIdResolver(baseType, config.getTypeFactory(), basicMetaData);
    }
}
