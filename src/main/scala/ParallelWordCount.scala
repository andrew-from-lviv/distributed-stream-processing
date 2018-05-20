package ucu.cs.parallel

import org.scalameter._

import scala.io.Source


object ParallelWordCount{

  trait Monoid[A]{
    def op(x: A, y: A): A
    def zero: A
  }

  def foldMapPar[A, B](xs: IndexedSeq[A],
                       from: Int, to: Int, m: Monoid[B])(f: A => B)
                      (implicit thresholdSize: Int): B =
    if (to-from < thresholdSize)
      foldMapSegment(xs, from, to, m)(f)
    else {
      val middle = from + (to - from) / 2
      val (l, r) = parallel(
        foldMapPar(xs, from, middle, m)(f)(thresholdSize),
        foldMapPar(xs, middle, to, m)(f)(thresholdSize))
      m.op(l, r)
    }

  def foldMapSegment[A,B](xs: IndexedSeq[A],from: Int, to: Int,
                          m: Monoid[B])(f: A => B): B = {
    var res = f(xs(from))
    var index = from + 1
    while (index < to){
      res = m.op(res, f(xs(index)))
      index = index + 1
    }
    res
  }

  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }

  def readTextSimple(filePath: String): String = {
    using(io.Source.fromFile(filePath)) { source => {
        source.getLines.mkString(" ") + '.'
      }
    }
  }

  val STOP_CHARS: String = "., ()[]{}!?@"
  sealed trait WordsCount
  case class StopChar(isStopChar: Boolean) extends WordsCount
  case class Part(lStopChar: Boolean, words: Int, rStopChar: Boolean) extends WordsCount

  val wordsCountMonoid = new Monoid[WordsCount] {
    override def op(a1: WordsCount, a2: WordsCount): WordsCount = (a1, a2) match {
      case (StopChar(s1), StopChar(s2)) =>
        StopChar(s1 || s2)
      case (StopChar(s1), Part(ls2, w2, rs2)) =>
        Part(s1 || ls2, w2, rs2)
      case (Part(ls1, w1, rs1), StopChar(s2)) =>
        Part(ls1, w1, s2 || rs1)
      case (Part(ls1, w1, rs1), Part(ls2, w2, rs2)) =>
        Part(ls1, w1 + w2 + (if (!rs1 && !ls2) 0 else 1), rs2)
    }
    override val zero = StopChar(false)
  }

  def wordCountFunction(c: Char): WordsCount = {
    if (STOP_CHARS contains c) StopChar(true) else Part(false, 0, false)
  }

  def wordsCountSeq(sentence: String): Int =
    foldMapSegment(sentence, 0, sentence.length, wordsCountMonoid)(wordCountFunction) match {
      case StopChar(s) => if(s) 1 else 0
      case Part(l, words, r) => (if(l) 1 else 0) + words + (if(r) 1 else 0)
    }

  implicit val threshold: Int = 1000
  def wordsCountPar(sentence: String): Int = {
    foldMapPar(sentence, 0, sentence.length, wordsCountMonoid)(wordCountFunction) match {
      case StopChar(s) => if(s) 1 else 0
      case Part(l, words, r) => (if(l) 1 else 0) + words + (if(r) 1 else 0)
    }
  }

  def main(args: Array[String]): Unit = {
    val source = readTextSimple("src/main/scala/textToCount.txt")
    println(s"Sequential:   ${wordsCountSeq(source)}")
    println(s"Parallel:     ${wordsCountPar(source)}")

    val standardConfig = config(
      Key.exec.minWarmupRuns -> 10,
      Key.exec.maxWarmupRuns -> 50,
      Key.exec.benchRuns -> 100,
      Key.verbose -> true
    ).withWarmer(new Warmer.Default)
    val seqTime = standardConfig.measure(wordsCountSeq(source))
    val parTime = standardConfig.measure(wordsCountPar(source))

    println(s"sequential time:  $seqTime")
    println(s"parallel time:    $parTime")
    println(s"speedup           ${seqTime.value / parTime.value}")
  }

}
