package org.amex.rtm

object RecidMidLoading {

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("Required 3 parameters, but got: "+ args.length)
      System.exit(1)
    }
    val indexname = args(0)
    val inputfolder = args(1)
    val host = args(2)


  }

}
