package org.apache.sedona.core.formatMapper.shapefileParser;


import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import java.io.Serializable;

public class ShapefileRDDProduct implements Serializable {
	private JavaRDD<Geometry> shapeRDD = null;

	public JavaRDD<Geometry> getShapeRDD() {
		return shapeRDD;
	}

	public void setShapeRDD(JavaRDD<Geometry> shapeRDD) {
		this.shapeRDD = shapeRDD;
	}

	/**
	* Gets the point RDD.
	* @return  the point RDD
	*/
	public JavaRDD<Point> getPointRDD() {
		return shapeRDD.flatMap(new FlatMapFunction<Geometry, Point>() {
			@Override
			public Iterator<Point> call(Geometry spatialObject) throws Exception {
				List<Point> result = new ArrayList<Point>();
				if (spatialObject instanceof MultiPoint) {
					MultiPoint multiObjects = (MultiPoint) spatialObject;
					for (int i = 0; i < multiObjects.getNumGeometries(); i++) {
						Point oneObject = (Point) multiObjects.getGeometryN(i);
						oneObject.setUserData(multiObjects.getUserData());
						result.add(oneObject);
					}
				} else if (spatialObject instanceof Point) {
					result.add((Point) spatialObject);
				} else {
					throw new Exception(
							"[ShapefileRDD][getPointRDD] the object type is not Point or MultiPoint type. It is "
									+ spatialObject.getGeometryType());
				}
				return result.iterator();
			}
		});
	}

	/**
	* Gets the polygon RDD.
	* @return  the polygon RDD
	*/
	public JavaRDD<Polygon> getPolygonRDD() {
		return shapeRDD.flatMap(new FlatMapFunction<Geometry, Polygon>() {
			@Override
			public Iterator<Polygon> call(Geometry spatialObject) throws Exception {
				List<Polygon> result = new ArrayList<Polygon>();
				if (spatialObject instanceof MultiPolygon) {
					MultiPolygon multiObjects = (MultiPolygon) spatialObject;
					for (int i = 0; i < multiObjects.getNumGeometries(); i++) {
						Polygon oneObject = (Polygon) multiObjects.getGeometryN(i);
						oneObject.setUserData(multiObjects.getUserData());
						result.add(oneObject);
					}
				} else if (spatialObject instanceof Polygon) {
					result.add((Polygon) spatialObject);
				} else {
					throw new Exception(
							"[ShapefileRDD][getPolygonRDD] the object type is not Polygon or MultiPolygon type. It is "
									+ spatialObject.getGeometryType());
				}
				return result.iterator();
			}
		});
	}

	/**
	* Gets the line string RDD.
	* @return  the line string RDD
	*/
	public JavaRDD<LineString> getLineStringRDD() {
		return shapeRDD.flatMap(new FlatMapFunction<Geometry, LineString>() {
			@Override
			public Iterator<LineString> call(Geometry spatialObject) throws Exception {
				List<LineString> result = new ArrayList<LineString>();
				if (spatialObject instanceof MultiLineString) {
					MultiLineString multiObjects = (MultiLineString) spatialObject;
					for (int i = 0; i < multiObjects.getNumGeometries(); i++) {
						LineString oneObject = (LineString) multiObjects.getGeometryN(i);
						oneObject.setUserData(multiObjects.getUserData());
						result.add(oneObject);
					}
				} else if (spatialObject instanceof LineString) {
					result.add((LineString) spatialObject);
				} else {
					throw new Exception(
							"[ShapefileRDD][getLineStringRDD] the object type is not LineString or MultiLineString type. It is "
									+ spatialObject.getGeometryType());
				}
				return result.iterator();
			}
		});
	}

	/**
	* Count.
	* @return  the long
	*/
	public long count() {
		return shapeRDD.count();
	}
}