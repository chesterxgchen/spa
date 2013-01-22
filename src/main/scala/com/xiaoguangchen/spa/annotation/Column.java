package com.xiaoguangchen.spa.annotation;

import java.lang.annotation.ElementType;

/**
 * Chester Chen (chesterxgchen@yahoo.com
 * User: cchen
 * Date: 10/29/11
 * Time: 7:49 PM
 */
@java.lang.annotation.Target({java.lang.annotation.ElementType.METHOD, java.lang.annotation.ElementType.FIELD,  ElementType.PARAMETER })
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
public @interface Column {

    java.lang.String value() default "" ;

}