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

package org.apache.sedona.core.formatMapper;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.sernetcdf.SerNetCDFUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import ucar.ma2.Array;
import ucar.nc2.dataset.NetcdfDataset;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class EarthdataHDFPointMapper.
 */
public class EarthdataHDFPointMapper
        implements FlatMapFunction<Iterator<String>, Geometry>
{

    private EarthdataHDFPointMapperProduct earthdataHDFPointMapperProduct = new EarthdataHDFPointMapperProduct();
	/**
     * The geolocation field.
     */
    private final String geolocationField = "Geolocation_Fields";
    /**
     * The longitude name.
     */
    private final String longitudeName = "Longitude";
    /**
     * The latitude name.
     */
    private final String latitudeName = "Latitude";
    /**
     * The data field name.
     */
    private final String dataFieldName = "Data_Fields";
    /**
     * The offset.
     */
    private int offset = 0;
    /**
     * The increment.
     */
    private int increment = 1;
    /**
     * The root group name.
     */
    private String rootGroupName = "MOD_Swath_LST";
    /**
     * The data variable name.
     */
    private String dataVariableName = "LST";
    /**
     * The longitude path.
     */
    private String longitudePath = "";
    /**
     * The latitude path.
     */
    private String latitudePath = "";
    /**
     * The data path.
     */
    private String dataPath = "";
    /**
     * The switch coordinate XY. By default, longitude is X, latitude is Y
     */
    private boolean switchCoordinateXY = false;

    /**
     * The url prefix.
     */
    private String urlPrefix = "";

    /**
     * Instantiates a new earthdata HDF point mapper.
     *
     * @param offset the offset
     * @param increment the increment
     * @param rootGroupName the root group name
     * @param dataVariableList the data variable list
     * @param dataVariableName the data variable name
     * @param switchCoordinateXY the switch coordinate XY
     */
    public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName, boolean switchCoordinateXY)
    {
        this.switchCoordinateXY = switchCoordinateXY;
		EarthdataHDFPointMapperExtracted2(offset, increment, rootGroupName, dataVariableList, dataVariableName);
    }

    /**
     * Instantiates a new earthdata HDF point mapper.
     *
     * @param offset the offset
     * @param increment the increment
     * @param rootGroupName the root group name
     * @param dataVariableList the data variable list
     * @param dataVariableName the data variable name
     */
    public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName)
    {
        EarthdataHDFPointMapperExtracted2(offset, increment, rootGroupName, dataVariableList, dataVariableName);
    }

    /**
     * Instantiates a new earthdata HDF point mapper.
     *
     * @param offset the offset
     * @param increment the increment
     * @param rootGroupName the root group name
     * @param dataVariableList the data variable list
     * @param dataVariableName the data variable name
     * @param switchCoordinateXY the switch coordinate XY
     * @param urlPrefix the url prefix
     */
    public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName,
            boolean switchCoordinateXY, String urlPrefix)
    {
        EarthdataHDFPointMapperExtracted(offset, increment, rootGroupName, dataVariableList, dataVariableName,
				urlPrefix, () -> {
					this.switchCoordinateXY = switchCoordinateXY;
				});
    }

    /**
     * Instantiates a new earthdata HDF point mapper.
     *
     * @param offset the offset
     * @param increment the increment
     * @param rootGroupName the root group name
     * @param dataVariableList the data variable list
     * @param dataVariableName the data variable name
     * @param urlPrefix the url prefix
     */
    public EarthdataHDFPointMapper(int offset, int increment, String rootGroupName, String[] dataVariableList, String dataVariableName,
            String urlPrefix)
    {
        EarthdataHDFPointMapperExtracted(offset, increment, rootGroupName, dataVariableList, dataVariableName,
				urlPrefix, () -> {
				});
    }

    @Override
    public Iterator<Geometry> call(Iterator<String> stringIterator)
            throws Exception
    {
        List<Geometry> hdfData = new ArrayList<Geometry>();
        while (stringIterator.hasNext()) {
            Array[] dataArrayList = earthdataHDFPointMapperProduct.dataArrayList(stringIterator, this.urlPrefix);
			String hdfAddress = stringIterator.next();
            NetcdfDataset netcdfSet = SerNetCDFUtils.loadNetCDFDataSet(urlPrefix + hdfAddress);
            Array longitudeArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, this.longitudePath);
            Array latitudeArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, this.latitudePath);
            Array dataArray = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, this.dataPath);
            int[] geolocationShape = longitudeArray.getShape();
            GeometryFactory geometryFactory = new GeometryFactory();
            for (int j = 0; j < geolocationShape[0]; j++) {
                for (int i = 0; i < geolocationShape[1]; i++) {
                    Point observation = observation(longitudeArray, latitudeArray, dataArray, dataArrayList,
							geometryFactory, j, i);
					hdfData.add(observation);
                }
            }
        }
        return hdfData.iterator();
    }

	private Point observation(Array longitudeArray, Array latitudeArray, Array dataArray, Array[] dataArrayList,
			GeometryFactory geometryFactory, int j, int i) {
		Coordinate coordinate = coordinate(longitudeArray, latitudeArray, dataArray, j, i);
		Point observation = geometryFactory.createPoint(coordinate);
		String userData = userData(dataArrayList, j, i);
		observation.setUserData(userData);
		return observation;
	}

	private String userData(Array[] dataArrayList, int j, int i) {
		String userData = "";
		for (int k = 0; k < earthdataHDFPointMapperProduct.getDataVariableList().length - 1; k++) {
			userData += SerNetCDFUtils.getDataAsym(dataArrayList[k], j, i, offset, increment) + " ";
		}
		userData += SerNetCDFUtils.getDataAsym(dataArrayList[earthdataHDFPointMapperProduct.getDataVariableList().length - 1], j, i, offset, increment);
		return userData;
	}

	private Coordinate coordinate(Array longitudeArray, Array latitudeArray, Array dataArray, int j, int i) {
		Coordinate coordinate = null;
		if (switchCoordinateXY) {
			coordinate = new Coordinate(SerNetCDFUtils.getDataSym(longitudeArray, j, i),
					SerNetCDFUtils.getDataSym(latitudeArray, j, i),
					SerNetCDFUtils.getDataAsym(dataArray, j, i, offset, increment));
		} else {
			coordinate = new Coordinate(SerNetCDFUtils.getDataSym(latitudeArray, j, i),
					SerNetCDFUtils.getDataSym(longitudeArray, j, i),
					SerNetCDFUtils.getDataAsym(dataArray, j, i, offset, increment));
		}
		return coordinate;
	}

	@FunctionalInterface
	private interface Interface0 {
		void apply();
	}

	private void EarthdataHDFPointMapperExtracted(int offset, int increment, String rootGroupName,
			String[] dataVariableList, String dataVariableName, String urlPrefix, Interface0 arg0) {
		arg0.apply();
				this.urlPrefix = urlPrefix;
				EarthdataHDFPointMapperExtracted3(offset, increment, rootGroupName, dataVariableList, dataVariableName);
	}

	private void EarthdataHDFPointMapperExtracted2(int offset, int increment, String rootGroupName,
			String[] dataVariableList, String dataVariableName) {
		EarthdataHDFPointMapperExtracted3(offset, increment, rootGroupName, dataVariableList, dataVariableName);
	}

	private void EarthdataHDFPointMapperExtracted3(int offset, int increment, String rootGroupName,
			String[] dataVariableList, String dataVariableName) {
		this.offset = offset;
		this.increment = increment;
		this.rootGroupName = rootGroupName;
		earthdataHDFPointMapperProduct.setDataVariableList(dataVariableList);
		this.dataVariableName = dataVariableName;
		this.longitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.longitudeName;
		this.latitudePath = this.rootGroupName + "/" + this.geolocationField + "/" + this.latitudeName;
		this.dataPath = this.rootGroupName + "/" + this.dataFieldName + "/" + this.dataVariableName;
		earthdataHDFPointMapperProduct.setDataPathList(new String[dataVariableList.length]);
		for (int i = 0; i < dataVariableList.length; i++) {
			earthdataHDFPointMapperProduct.getDataPathList()[i] = this.rootGroupName + "/" + this.dataFieldName + "/" + dataVariableList[i];
		}
	}
}
