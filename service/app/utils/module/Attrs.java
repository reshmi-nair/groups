package utils.module;

import org.sunbird.util.JsonKey;
import play.libs.typedmap.TypedKey;

public class Attrs {
    public static final TypedKey<String> USERID = TypedKey.<String>create(JsonKey.USER_ID);
}
