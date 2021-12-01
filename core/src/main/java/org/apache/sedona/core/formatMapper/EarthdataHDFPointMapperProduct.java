package org.apache.sedona.core.formatMapper;


import ucar.ma2.Array;
import java.util.Iterator;
import ucar.nc2.dataset.NetcdfDataset;
import org.datasyslab.sernetcdf.SerNetCDFUtils;

public class EarthdataHDFPointMapperProduct {
	private String[] dataVariableList;
	private String[] dataPathList;

	public String[] getDataVariableList() {
		return dataVariableList;
	}

	public void setDataVariableList(String[] dataVariableList) {
		this.dataVariableList = dataVariableList;
	}

	public String[] getDataPathList() {
		return dataPathList;
	}

	public void setDataPathList(String[] dataPathList) {
		this.dataPathList = dataPathList;
	}

	public Array[] dataArrayList(Iterator<String> stringIterator, String thisUrlPrefix) {
		String hdfAddress = stringIterator.next();
		NetcdfDataset netcdfSet = SerNetCDFUtils.loadNetCDFDataSet(thisUrlPrefix + hdfAddress);
		Array[] dataArrayList = new Array[this.dataVariableList.length];
		for (int i = 0; i < this.dataVariableList.length; i++) {
			dataArrayList[i] = SerNetCDFUtils.getNetCDF2DArray(netcdfSet, dataPathList[i]);
		}
		return dataArrayList;
	}
}