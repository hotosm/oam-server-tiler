package org.hotosm.oam

import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }

import com.esotericsoftware.kryo.Kryo

class KryoRegistrator extends SparkKryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    (new geotrellis.spark.io.hadoop.KryoRegistrator).registerClasses(kryo)

    kryo.register(classOf[OrderedImage])
  }
}
