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

package org.apache.sedona.core.formatMapper.shapefileParser;

import org.apache.sedona.core.formatMapper.shapefileParser.boundary.BoundBox;
import org.apache.sedona.core.formatMapper.shapefileParser.boundary.BoundaryInputFormat;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.TypeUnknownException;
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.PrimitiveShape;
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.ShapeInputFormat;
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.ShapeKey;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class ShapefileRDD.
 */
@Deprecated
public class ShapefileRDD
        implements Serializable
{

    private ShapefileRDDProduct shapefileRDDProduct = new ShapefileRDDProduct();
	/**
     * The geometry factory.
     */
    public static GeometryFactory geometryFactory = new GeometryFactory();
    /**
     * The Constant PrimitiveToShape.
     */
    private static final Function<Tuple2<ShapeKey, PrimitiveShape>, Geometry> PrimitiveToShape
            = new Function<Tuple2<ShapeKey, PrimitiveShape>, Geometry>()
    {
        public Geometry call(Tuple2<ShapeKey, PrimitiveShape> primitiveTuple)
        {
            Geometry shape = null;
            // parse bytes to shape
            try {
                shape = primitiveTuple._2().getShape(geometryFactory);
            }
            catch (TypeUnknownException e) {
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                return shape;
            }
        }
    };
    /**
     * The Print shape.
     */
    private final VoidFunction<Geometry> PrintShape = new VoidFunction<Geometry>()
    {
        public void call(Geometry shape)
                throws Exception
        {
            System.out.println(shape.toText());
        }
    };
    /**
     * bounding box.
     */
    private BoundBox boundBox = null;

    /**
     * ShapefileRDD.
     *
     * @param sparkContext the spark context
     * @param filePath the file path
     */
    public ShapefileRDD(JavaSparkContext sparkContext, String filePath)
    {
        boundBox = new BoundBox();
        JavaPairRDD<ShapeKey, PrimitiveShape> shapePrimitiveRdd = sparkContext.newAPIHadoopFile(
                filePath,
                ShapeInputFormat.class,
                ShapeKey.class,
                PrimitiveShape.class,
                sparkContext.hadoopConfiguration()
        );
        shapefileRDDProduct.setShapeRDD(shapePrimitiveRdd.map(PrimitiveToShape));
    }

    /**
     * Gets the shape RDD.
     *
     * @return the shape RDD
     */
    public JavaRDD<Geometry> getShapeRDD()
    {
        return this.shapefileRDDProduct.getShapeRDD();
    }

    /**
     * Gets the point RDD.
     *
     * @return the point RDD
     */
    public JavaRDD<Point> getPointRDD()
    {
        return shapefileRDDProduct.getPointRDD();
    }

    /**
     * Gets the polygon RDD.
     *
     * @return the polygon RDD
     */
    public JavaRDD<Polygon> getPolygonRDD()
    {
        return shapefileRDDProduct.getPolygonRDD();
    }

    /**
     * Gets the line string RDD.
     *
     * @return the line string RDD
     */
    public JavaRDD<LineString> getLineStringRDD()
    {
        return shapefileRDDProduct.getLineStringRDD();
    }

    /**
     * Count.
     *
     * @return the long
     */
    public long count()
    {
        return shapefileRDDProduct.count();
    }

    /**
     * read and merge bound boxes of all shapefiles user input, if there is no, leave BoundBox null;
     */
    public BoundBox getBoundBox(JavaSparkContext sc, String inputPath)
    {
        // read bound boxes into memory
        JavaPairRDD<Long, BoundBox> bounds = sc.newAPIHadoopFile(
                inputPath,
                BoundaryInputFormat.class,
                Long.class,
                BoundBox.class,
                sc.hadoopConfiguration()
        );
        // merge all into one
        bounds = bounds.reduceByKey(new Function2<BoundBox, BoundBox, BoundBox>()
        {
            @Override
            public BoundBox call(BoundBox box1, BoundBox box2)
                    throws Exception
            {
                return BoundBox.mergeBoundBox(box1, box2);
            }
        });
        // if there is a result assign it to variable : boundBox
        if (bounds.count() > 0) {
            return new BoundBox(bounds.collect().get(0)._2());
        }
        else { return null; }
    }
}