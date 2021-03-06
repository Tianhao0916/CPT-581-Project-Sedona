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

package org.apache.sedona.python.wrapper.translation

import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.Output
import org.apache.sedona.core.geometryObjects.Circle
import org.apache.sedona.python.wrapper.SerializationException
import org.apache.sedona.python.wrapper.translation.serde.PythonGeometrySerialization
import org.apache.spark.sql.sedona_sql.{sedonaSerializer, userSerializerType}
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, Point, Polygon}
import org.locationtech.jts.io.WKBReader


private[python] case class PythonGeometrySerializer() extends Serializable {

  /*
      Translates JTS geometry to byte array which then will be decoded to Python shapely geometry objects
      Process needs two steps:
      - Translate user attributes using UTF-8 encoding
   */


  def serialize(geom: Geometry): Array[Byte] = {
    geom match {
      case geometry: Circle => CircleSerializer(geometry).serialize
      case geometry: Geometry => GeometrySerializer(geometry).serialize
    }


  }

  def deserialize: Array[Byte] => Geometry = (values: Array[Byte]) => {
    val isCircle = values.head.toInt
    val valuesLength = values.length

    if (isCircle == 1) {
      val geom =  PythonGeometrySerialization.deserialize(values.slice(9, valuesLength))
      val radius = ByteBuffer.wrap(values.slice(1, 9)).getDouble()
      new Circle(geom, radius)
    }
    else if (isCircle == 0) {
      PythonGeometrySerialization.deserialize(values.slice(1, valuesLength))
    }
    else throw SerializationException("Can not deserialize object")

  }
}