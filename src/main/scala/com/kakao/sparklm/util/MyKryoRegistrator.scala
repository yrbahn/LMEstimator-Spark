package com.kakao.sparklm.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[java.lang.Class[_]])
    kryo.register(classOf[com.kakao.sparklm.util.BWList])
    kryo.register(classOf[scala.collection.mutable.ListBuffer[_]])
    kryo.register(classOf[scala.collection.immutable.TreeSet[_]])
    kryo.register(Class.forName("scala.math.Ordering$String$"))
    kryo.register(Class.forName("scala.collection.immutable.RedBlackTree$BlackTree"))
    kryo.register(Class.forName("scala.collection.immutable.RedBlackTree$RedTree"))
    kryo.register(Class.forName("scala.math.Ordering$$anon$9"))
    //kryo.register(Class.forName("com.kakao.sparklm.rdd.SentenceRDD$$anonfun$6"))
    kryo.register(Class.forName("com.kakao.sparklm.rdd.SentenceRDD$$anonfun$5"))
    kryo.register(Class.forName("scala.math.Ordering$$anonfun$by$1"))
    kryo.register(Class.forName("scala.math.Ordering$$anon$11"))
    kryo.register(Class.forName("scala.math.Ordering$Int$"))
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[com.kakao.sparklm.util.DiscountMap])
    kryo.register(classOf[Array[scala.collection.immutable.Vector[_]]])
  }
}