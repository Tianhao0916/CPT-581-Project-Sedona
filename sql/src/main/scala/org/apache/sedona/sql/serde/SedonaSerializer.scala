/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.sql.serde

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.sedona.core.enums.SerializerType
import org.apache.sedona.core.serde.GeometrySerde
import org.apache.sedona.core.serde.WKB.WKBGeometrySerde
import org.apache.sedona.core.serde.shape.ShapeGeometrySerde
import org.apache.spark.sql.catalyst.util.ArrayData
import org.locationtech.jts.geom.Geometry
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.spark.sql.catalyst.util.ArrayData.toArrayData

class SedonaSerializer(geometrySerde: GeometrySerde) {

  def serialize(geometry: Geometry): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val kryo = new Kryo()
    val output = new Output(out)
    geometrySerde.write(kryo, output, geometry)
    output.close()
    out.toByteArray
  }

  def deserialize(values: ArrayData): Geometry = {
    deserialize(values.toByteArray())
  }

  def deserialize(values: Array[Byte]): Geometry = {
    val in = new ByteArrayInputStream(values)
    val kryo = new Kryo()
    val input = new Input(in)
    val geometry = geometrySerde.read(kryo, input, classOf[Geometry])
    input.close()
    geometry.asInstanceOf[Geometry]
  }

}

object SedonaSerializer {

  def apply(serializerType: SerializerType): SedonaSerializer = {
    new SedonaSerializer(getSerializer(serializerType))
  }

  def getSerializer(serializerType: SerializerType): GeometrySerde = {

    serializerType match {
      case SerializerType.SHAPE => new ShapeGeometrySerde()
      case SerializerType.WKB => new WKBGeometrySerde()
      case _ => new ShapeGeometrySerde()
    }

  }
}
