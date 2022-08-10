package com.kanlon.utils;

import java.util.Collection;
import java.util.Map;

/**
 * 集合工具类
 *
 * @author zhangcanlong
 * @since 2022/8/10 21:17
 **/
public class CollectionUtils {

    private CollectionUtils() {}

    /**
     * 判断是否为空
     *
     * @param collection 集合
     * @return booleankon
     */
    public static boolean isEmpty(Collection<?> collection) {
        return (collection == null || collection.isEmpty());
    }

    /**
     * Return {@code true} if the supplied Map is {@code null} or empty.
     * Otherwise, return {@code false}.
     *
     * @param map the Map to check
     * @return whether the given Map is empty
     */
    public static boolean isEmpty(Map<?, ?> map) {
        return (map == null || map.isEmpty());
    }


}
