package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  val aBool: Boolean = false

  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  val theUnit = println("Hello, Scala") // Unit = "no meaningful value" = void in other languages

  def myFunction(x: Int) = 42

  // OOP
  class Animal

  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton

  // companion (of class Carnivore)
  object Carnivore

  //generics
  trait MyList[A]

  //method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  // Anonymous or lambda function
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)

  // map, flatMap, filter - can receive functions are arguments
  val processedList = List(1, 2, 3).map(incrementer)
  // is the same as
  val processedList2 = List(1, 2, 3).map(x => incrementer(x))


  println(processedList2)

  //Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }


  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "Some returned value"
    case _ => "Something else"
  }

  // Futures

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }
  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(exception) => println(s"Failed $exception")
  }

  // Partial functions
  //  val aPartialFunction = (x: Int) => x match {
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // implicits
  //auto-injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgument


  // implicit conversions - implicit def
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  // here String is automatically converted to a Person, by calling `greet`
  "Bob".greet //fromStringToPerson("Bob").greet

  // implicit converstion - implicit classes
  implicit class Dog(name: String) {
    def bark = println("Bark")
  }

  "Lasso".bark

  /*
   - local scope (like Person and Dog)
   - imported scope (like import scala.concurrent.ExecutionContext.Implicits.global; where global is linked to Future
   - companion objects of the type involved in the method call
   */
  List(1,2,3).sorted

}
