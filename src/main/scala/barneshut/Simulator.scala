package barneshut

import java.awt._
import java.awt.event._
import javax.swing._
import javax.swing.event._
import scala.collection.parallel.TaskSupport
import scala.collection.parallel.Combiner
import scala.collection.parallel.mutable.ParHashSet
import common._
import scala.collection._

class Simulator(val taskSupport: TaskSupport, val timeStats: TimeStatistics) {

  def updateBoundaries(boundaries: Boundaries, body: Body): Boundaries = {
    var minx = boundaries.minX
    var maxx = boundaries.maxX
    var miny = boundaries.minY
    var maxy = boundaries.maxY
    
    if (body.x < minx) minx = body.x
    if (body.x > maxx) maxx = body.x
    if (body.y < miny) miny = body.y
    if (body.y > maxy) maxy = body.y
    
    var b = new Boundaries()
    b.minX = minx
    b.minY = miny
    b.maxX = maxx
    b.maxY = maxy
    b
  }

  def mergeBoundaries(a: Boundaries, b: Boundaries): Boundaries = {
    var minx = a.minX
    var maxx = a.maxX
    var miny = a.minY
    var maxy = a.maxY
    
    if (b.minX < minx) minx = b.minX
    if (b.maxX > maxx) maxx = b.maxX
    if (b.minY < miny) miny = b.minY
    if (b.maxY > maxy) maxy = b.maxY
    
    var c = new Boundaries()
    c.minX = minx
    c.minY = miny
    c.maxX = maxx
    c.maxY = maxy
    c
  }

  def computeBoundaries(bodies: Seq[Body]): Boundaries = timeStats.timed("boundaries") {
    val parBodies = bodies.par
    parBodies.tasksupport = taskSupport
    parBodies.aggregate(new Boundaries)(updateBoundaries, mergeBoundaries)
  }

  def computeSectorMatrix(bodies: Seq[Body], boundaries: Boundaries): SectorMatrix = timeStats.timed("matrix") {
    val parBodies = bodies.par
    parBodies.tasksupport = taskSupport
    
    def addToSM(sm: SectorMatrix, b: Body) : SectorMatrix = {
      sm += b
    }
    def combo(sm1: SectorMatrix, sm2: SectorMatrix) : SectorMatrix = {
      sm1.combine(sm2)
    }
    
    // def aggregate [B] (z: B)(seqop: (B, A) ⇒ B, combop: (B, B) ⇒ B): B
    // def aggregate [B] (z: SectorMatrix)(seqop: (SectorMatrix, Body) ⇒ SectorMatrix, combop: (SectorMatrix, SectorMatrix) ⇒ SectorMatrix): SectorMatrix
    parBodies.aggregate(new SectorMatrix(boundaries, barneshut.SECTOR_PRECISION)) (addToSM,  combo)
  }

  def computeQuad(sectorMatrix: SectorMatrix): Quad = timeStats.timed("quad") {
    sectorMatrix.toQuad(taskSupport.parallelismLevel)
  }

  def updateBodies(bodies: Seq[Body], quad: Quad): Seq[Body] = timeStats.timed("update") {
    val parBodies = bodies.par
    parBodies.tasksupport = taskSupport
    
    def seqop(sq: Vector[Body], b: Body) : Vector[Body] = {
      sq :+ (b.updated(quad))
    }
    def combo(sq1: Vector[Body], sq2: Vector[Body]) : Vector[Body] = {
      sq1 ++ sq2 
    }
    
    // def aggregate [B] (z: B)(seqop: (B, A) ⇒ B, combop: (B, B) ⇒ B): B
    // def aggregate [B] (z: Vector[Body])(seqop: (Vector[Body], Body) ⇒ Vector[Body], combop: (Vector[Body], Vector[Body]) ⇒ Vector[Body]): Vector[Body]
    parBodies.aggregate(Vector[Body]()) (seqop, combo)
  }

  def eliminateOutliers(bodies: Seq[Body], sectorMatrix: SectorMatrix, quad: Quad): Seq[Body] = timeStats.timed("eliminate") {
    def isOutlier(b: Body): Boolean = {
      val dx = quad.massX - b.x
      val dy = quad.massY - b.y
      val d = math.sqrt(dx * dx + dy * dy)
      // object is far away from the center of the mass
      if (d > eliminationThreshold * sectorMatrix.boundaries.size) {
        val nx = dx / d
        val ny = dy / d
        val relativeSpeed = b.xspeed * nx + b.yspeed * ny
        // object is moving away from the center of the mass
        if (relativeSpeed < 0) {
          val escapeSpeed = math.sqrt(2 * gee * quad.mass / d)
          // object has the espace velocity
          -relativeSpeed > 2 * escapeSpeed
        } else false
      } else false
    }

    def outliersInSector(x: Int, y: Int): Combiner[Body, ParHashSet[Body]] = {
      val combiner = ParHashSet.newCombiner[Body]
      combiner ++= sectorMatrix(x, y).filter(isOutlier)
      combiner
    }

    val sectorPrecision = sectorMatrix.sectorPrecision
    val horizontalBorder = for (x <- 0 until sectorPrecision; y <- Seq(0, sectorPrecision - 1)) yield (x, y)
    val verticalBorder = for (y <- 1 until sectorPrecision - 1; x <- Seq(0, sectorPrecision - 1)) yield (x, y)
    val borderSectors = horizontalBorder ++ verticalBorder

    // compute the set of outliers
    val parBorderSectors = borderSectors.par
    parBorderSectors.tasksupport = taskSupport
    val outliers = parBorderSectors.map({ case (x, y) => outliersInSector(x, y) }).reduce(_ combine _).result

    // filter the bodies that are outliers
    val parBodies = bodies.par
    parBodies.filter(!outliers(_)).seq
  }

  def step(bodies: Seq[Body]): (Seq[Body], Quad) = {
    // 1. compute boundaries
    val boundaries = computeBoundaries(bodies)
    
    // 2. compute sector matrix
    val sectorMatrix = computeSectorMatrix(bodies, boundaries)

    // 3. compute quad tree
    val quad = computeQuad(sectorMatrix)
    
    // 4. eliminate outliers
    val filteredBodies = eliminateOutliers(bodies, sectorMatrix, quad)

    // 5. update body velocities and positions
    val newBodies = updateBodies(filteredBodies, quad)
    
    (newBodies, quad)
  }

}
