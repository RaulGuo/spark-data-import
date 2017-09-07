package com.proud.dc.test

import scala.util.{Try, Success, Failure}

object TryTest {
  def main(args: Array[String]): Unit = {
    val v = divide(10, 0)
    v match {
      case Success(i) => println("divide success"+i)
      case Failure(t) => {
        println("divide fail"+t.getClass)
      }
    }
  }
  
  def divide(a:Int, b:Int): Try[Int] = {
    Try({
      if(b != 0) a/b else {
        throw new Exception("狗带")
      }
    })
  }
}