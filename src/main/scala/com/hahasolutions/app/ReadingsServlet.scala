package com.hahasolutions.app

import org.scalatra.ScalatraServlet

import java.util.Date
import java.sql.Timestamp

import com.mchange.v2.c3p0.ComboPooledDataSource
import java.util.Properties
import org.slf4j.LoggerFactory

import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._

import scala.slick.driver.PostgresDriver.simple._
import Database.threadLocalSession

case class DeviceType(id: Option[Int], name: String, version: String)
 
object DeviceTypes extends Table[DeviceType]("device_types") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def name = column[String]("name", O.NotNull)
  def version = column[String]("version")

  def * = id.? ~ name ~ version <> (DeviceType, DeviceType.unapply _)
  def forInsert = name ~ version <> (
    { t => DeviceType(None, t._1, t._2) }, 
    { (dt: DeviceType) => Some((dt.name, dt.version)) })
case class Device(id: Option[Int], mac_addr: String, device_type_id: Int, manufactured_at: Timestamp, registered_at: Option[Timestamp])
}

case class Device(id: Option[Int], mac_addr: String, device_type_id: Int, manufactured_at: Timestamp, registered_at: Option[Timestamp])
 
object Devices extends Table[Device]("devices") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def mac_addr = column[String]("mac_addr")
  def device_type_id = column[Int]("device_type_id")
  def manufactured_at = column[Timestamp]("manufactured_at")
  def registered_at = column[Option[Timestamp]]("registered_at")

  def * = id.? ~ mac_addr ~ device_type_id ~ manufactured_at ~ registered_at <> (Device, Device.unapply _)
  def forInsert = mac_addr ~ device_type_id ~ manufactured_at ~ registered_at <> (
    { t => Device(None, t._1, t._2, t._3, t._4) },
    { (d: Device) => Some((d.mac_addr, d.device_type_id, d.manufactured_at, d.registered_at)) })
 
  // A reified foreign key relation that can be navigated to create a join
  def deviceType = foreignKey("device_type_fk", device_type_id, DeviceTypes)(_.id)
}

case class Reading(id: Option[Int], device_mac_addr: String, value: String, created_at: Timestamp)
 
object Readings extends Table[Reading]("readings"){
 
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def device_mac_addr = column[String]("device_mac_addr", O.NotNull)
  def value = column[String]("value", O.NotNull)
  def created_at = column[Timestamp]("created_at")

  def * = id.? ~ device_mac_addr ~ value ~ created_at <> (Reading, Reading.unapply _)
  def forInsert = device_mac_addr ~ value ~ created_at <> (
    { t => Reading(None, t._1, t._2, t._3) }, 
    { (r: Reading) => Some((r.device_mac_addr, r.value, r.created_at)) })
 
  // A reified foreign key relation that can be navigated to create a join
  def device = foreignKey("device_fk", device_mac_addr, Devices)(_.mac_addr)
}


trait SlickSupport extends ScalatraServlet {

  val logger = LoggerFactory.getLogger(getClass)

  val cpds = {
    val props = new Properties
    props.load(getClass.getResourceAsStream("/c3p0.properties"))
    val cpds = new ComboPooledDataSource
    cpds.setProperties(props)
    logger.info("Created c3p0 connection pool")
    cpds
  }

  def closeDbConnection() {
    logger.info("Closing c3po connection pool")
    cpds.close
  }

  val db = Database.forDataSource(cpds)

  override def destroy() {
    super.destroy()
    closeDbConnection
  }
}

class ReadingsServlet extends ScalatraServlet with SlickSupport with JacksonJsonSupport {
  // Sets up automatic case class to JSON output serialization, required by
  // the JValueResult trait.
  protected implicit val jsonFormats: Formats = DefaultFormats

  get("/db/create-tables") {
    db withSession {
      (DeviceTypes.ddl ++ Devices.ddl ++ Readings.ddl).create
    }
  }

  get("/db/load-data") {
    val date = new Date
    val timestamp = new Timestamp(date.getTime)
    db withSession {
      // Insert some device types
      DeviceTypes.forInsert.insertAll(
        DeviceType(None, "NIBP device", "1.0"),
        DeviceType(None, "SpO2 device", "1.0")
      )

      // Insert some devices (using JDBC's batch insert feature, if supported by the DB)
      Devices.forInsert.insertAll(
        Device(None, "123456789012", 4, timestamp, None),
        Device(None, "923456789012", 4, timestamp, None),
        Device(None, "823456789012", 5, timestamp, None)
      )
    }
  }

  get("/db/drop-tables") {
    db withSession {
      (DeviceTypes.ddl ++ Devices.ddl ++ Readings.ddl).drop
    }
  }

  get("/devices") {
    db withSession {
      val q3 = for {
        d <- Devices
        dt <- d.deviceType
      } yield (d.mac_addr.asColumnOf[String], dt.name.asColumnOf[String])

      // send the json response 
      contentType = formats("json")
      q3.list.map { case (s1, s2) => Map("MacAddr" -> s1, "DeviceType" -> s2) }
    }
  }

  get("/readings") {
    db withSession {
      val q3 = for {
        r <- Readings
        d <- r.device
        dt <- d.deviceType
      } yield (r.value, d.mac_addr.asColumnOf[String], dt.name.asColumnOf[String])

      // send the json response 
      contentType = formats("json")
      q3.list.map { case (s0, s1, s2) => Map("Reading" -> s0, "MacAddr" -> s1, "DeviceType" -> s2) }
    }
  }

  get("/devices/:device_mac_addr/readings") {
    db withSession {
      val q3 = for {
        r <- Readings
        d <- r.device if d.mac_addr === params("device_mac_addr")
        dt <- d.deviceType
      } yield (r.value, d.mac_addr.asColumnOf[String], dt.name.asColumnOf[String])

      // send the json response 
      contentType = formats("json")
      q3.list.map { case (s0, s1, s2) => Map("Reading" -> s0, "MacAddr" -> s1, "DeviceType" -> s2) }
    }
  }

  post("/devices/:device_mac_addr/readings") {
    db withSession {
      val date = new Date
      val timestamp = new Timestamp(date.getTime)
      // For parsedBody to work, content type should be application/json 
      val value = (parsedBody \ "value").extract[String]
      val reading = Reading(None, params("device_mac_addr"), value, timestamp)
      Readings.forInsert.insert(reading)
      // send the json response 
      contentType = formats("json")
      Map("status" -> "ok")
    }
  }
}

