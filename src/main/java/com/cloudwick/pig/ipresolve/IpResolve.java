package com.cloudwick.pig.ipresolve;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bijay on 1/6/15.
 */
public class IpResolve extends EvalFunc<String> {
  Logger logger = LoggerFactory.getLogger(IpResolve.class);

  DatabaseReader reader;
  private File ipDatabase;

  public IpResolve(String fileName) {
    this.ipDatabase = new File(fileName);
    try {
      this.reader = new DatabaseReader.Builder(ipDatabase).build();
    } catch (IOException e) {
      System.out.println("Database reader cannot be created");
      e.printStackTrace();
    }
  }


  public String exec(Tuple tuple) throws IOException {
    String ip = (String) tuple.get(0);
    String countryName = null;
    try {
      CityResponse response = reader.city(InetAddress.getByName(ip));
      countryName = response.getCountry().getName();
      if (countryName == null && countryName.length() == 0) {
        logger.warn("ipAddress cannot be resolved");
        return null;
      }

    } catch (GeoIp2Exception e) {
      e.printStackTrace();
    }
    return countryName;
  }


  @Override
  public List<String> getCacheFiles() {
    List<String> list = new ArrayList<String>(1);
    list.add(ipDatabase + "#ip_lookup");
    return list;
  }
}
