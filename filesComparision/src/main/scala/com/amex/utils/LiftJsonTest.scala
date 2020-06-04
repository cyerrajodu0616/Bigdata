package main.scala.com.amex.utils

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write


case class Person(name: String, address: Address)
case class Address(city: String, state: String)

object LiftJsonTest {

  val p = Person("Alvin Alexander", Address("Talkeetna", "AK"))
  // create a JSON string from the Person, then print it
  implicit val formats = DefaultFormats
  val jsonString = write(p)
  println(jsonString)

}
