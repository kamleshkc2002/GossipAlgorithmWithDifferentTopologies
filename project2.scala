/*
Kamlesh Kumar Chhetty

Konguvel Balakrishnan
*/

import akka.actor._
import scala.math._
import scala.util.Random

case class startGossiping(actor:ActorRef, algorithm:String, numNodes:Int)
case class CheckForTermination(myID:Int, gossipCount:Int, starttime:Long)

case class Initialize(uniqueId:Int, actorslist:List[ActorRef], neighbor:List[Int], master:ActorRef)
case class Gossip(id:Int, starttime:Long)
case class Pushsum(sum:Double, weight:Double, starttim:Long)

object project2 {
	def main (args: Array[String]): Unit = {
		if (args.length != 3) {
			println("Enter number of Nodes, topology and Algorithm as arguments")
		}
		else {
			var numNodes:Int = args(0).toInt
			var topology:String = args(1)
			var algorithm:String = args(2)
			var numNodesSqrt:Double = 0

			if (topology.equalsIgnoreCase("2d") || topology.equalsIgnoreCase("imp2d")) {
				numNodes = getNextPerfectSquare(numNodes)
				numNodesSqrt = math.sqrt(numNodes.toDouble)
			}

			val system = ActorSystem("GossipSimulator")

			var master:ActorRef = system.actorOf(Props[MasterClass])
			var actorslist:List[ActorRef] = Nil

			var i:Int = 0
			while (i<numNodes){
				actorslist ::= system.actorOf(Props[ActorClass])
				i += 1
			}

			if (topology.equalsIgnoreCase("line")){
				var i:Int = 0
				while (i < actorslist.length){
					var neighbors:List[Int] = Nil

					if (i > 0) neighbors ::= (i-1)
					if (i<actorslist.length-1) neighbors ::= (i+1)

					actorslist(i) ! Initialize(i, actorslist, neighbors, master)

					i += 1
				}
			}
			else if (topology.equalsIgnoreCase("2d")){
				var i:Int = 0
				while (i < actorslist.length){
					var neighbors:List[Int] = Nil
					var topn = i-numNodesSqrt
					var botn = i+numNodesSqrt
					var leftn = i-1
					var rightn = i+1

					if (topn >= 0) neighbors ::= topn.toInt
					if (botn < numNodes) neighbors ::= botn.toInt
					if (i%numNodesSqrt > 0) neighbors ::= leftn
					if (i%numNodesSqrt < (numNodesSqrt-1)) neighbors ::= rightn

					actorslist(i) ! Initialize(i, actorslist, neighbors, master)

					i += 1
				}
			}
			else if (topology.equalsIgnoreCase("imp2d")){
				var i:Int = 0
				while (i < actorslist.length){
					var neighbors:List[Int] = Nil

					var topn = i-numNodesSqrt
					var botn = i+numNodesSqrt
					var leftn = i -1
					var rightn = i+1

					if (topn >= 0) { neighbors ::= topn.toInt }
					if (botn < numNodes) { neighbors ::= botn.toInt }
					if (i%numNodesSqrt > 0) { neighbors ::= leftn }
					if (i%numNodesSqrt < (numNodesSqrt-1)) { neighbors ::= rightn }

					var randomneighbor:Int = -1
					while (randomneighbor == -1) {
						randomneighbor = Random.nextInt(actorslist.length)
						for (x <- neighbors) {
							if (randomneighbor == x) {
								randomneighbor = -1
							}
						}
					}

					neighbors ::= (randomneighbor)

					actorslist(i) ! Initialize(i, actorslist, neighbors, master)

					i += 1
				}
			}

			else if (topology.equalsIgnoreCase("full")) {
				var i:Int = 0

				while (i < actorslist.length) {
					var neighbors:List[Int] = Nil
					var j:Int = 0
					while (j < actorslist.length){
						if (j != i) {
							neighbors ::= j
						}
						j += 1
					}
					actorslist(i) ! Initialize(i, actorslist, neighbors, master)
					i += 1
				}
			}
			else {
				println("INVALID topology mentioned!")
				sys.exit(0)
			}

			master ! startGossiping(actorslist(0), algorithm, numNodes)
		}
		def getNextPerfectSquare (num:Int):Int = {
			var bi:Int = num
			while(math.sqrt(bi.toDouble)%1 != 0){
				bi += 1
			}
			return bi
		}
	}
}

case class ActorClass () extends Actor {
	var rumorthreshhold = 10
	var myID:Int = 0
	var actorslist:List[ActorRef] = Nil

	var master:ActorRef = null
	var neighbors:List[Int] = Nil
	var rumorcounter:Int = 0
	var negliblediffcount:Int = 0

	var allnodes:List[Int] =Nil

	var s:Double = 0
	var w:Double = 0

	def receive = {
	case Initialize(uniqueId:Int, allNodes:List[ActorRef], neighborList:List[Int], masterarg:ActorRef) => {
		neighbors = neighbors ::: neighborList
		myID = uniqueId
		master = masterarg
		s = uniqueId
		actorslist = allNodes
	}


	case Gossip(callerid:Int, starttime:Long) => {
		if (rumorcounter < rumorthreshhold) {

			if (callerid != myID) {
				rumorcounter += 1
				master ! CheckForTermination(myID, rumorcounter, starttime)
			}

			var randomPlayer:Int = 0

			randomPlayer = Random.nextInt(neighbors.length)
			actorslist(neighbors(randomPlayer)) ! Gossip(myID, starttime)
			self ! Gossip(myID, starttime)
		}
		else {
			//println("stopping ->"+myID)
			context.stop(self)
		}
	}

	case Pushsum(news:Double, neww:Double, starttime:Long) => {
//		println("inside pushsum-->negcount="+negliblediffcount)
		rumorcounter += 1
		var oldratio:Double = s/w
//		println("oldratio="+oldratio+"s="+s+"w="+w)
		s += news
		w += neww
		s = s/2
		w = w/2
		var newratio:Double = s/w
//		println("newratio="+newratio+"s="+s+"w="+w)
		if ((rumorcounter == 1) || (Math.abs((oldratio-newratio)) > math.pow(10, -10))) {
			//println("Inside If--->")
			negliblediffcount=0
			var randomPlayer = Random.nextInt(neighbors.length)
			actorslist(neighbors(randomPlayer)) ! Pushsum(s, w, starttime)

		} else {
			//println("Inside Else---->")
			negliblediffcount += 1
			if (negliblediffcount > 3) {
				println(System.currentTimeMillis-starttime)
				sys.exit(0)
			} else {
				var randomPlayer = Random.nextInt(neighbors.length)
				actorslist(neighbors(randomPlayer)) ! Pushsum(s, w, starttime)
			}
		}
	}
	}
}

class MasterClass extends Actor{
	var numNodes:Int = 0
	var rumorthreshhold:Int = 10
	var visitednodes:List[Int] = Nil

	def receive = {
		case startGossiping(actor:ActorRef, algorithm:String, count:Int) => {
			numNodes = count
			//Topology is created. Note the starting time.
			var b:Long = System.currentTimeMillis

			if (algorithm.equalsIgnoreCase("gossip")) {
				actor ! Gossip(-1, b)
			}
			else if (algorithm.equalsIgnoreCase("push-sum")){
					actor ! Pushsum(0, 1, b)
			}
			else {
				println("INVALID algorithm mentioned!")
				sys.exit(0)
			}
		}

		case CheckForTermination(id:Int, count:Int, starttime:Long) => {
			var flag:Int = 1
			var i:Int = 0
			//println("-->content in status list:")
			while(i < visitednodes.length) {
				//println("-->"+visitednodes(i))
				if(visitednodes(i) == id) {
					flag = 0
				}
				i += 1
			}

			if (flag == 1) {
				visitednodes ::= id
				//print("Number of nodes visited="+visitednodes.length+"\r")
				//println("Node first visited-->"+id)
			}

			if(visitednodes.length == numNodes) {
				/*
				println("\nNodes visited = ")
				var i:Int = 0
				while (i < visitednodes.length) {
					print(visitednodes(i)+"<-")
					i += 1
				}
				*/
				//println("\n")
				println(System.currentTimeMillis-starttime)
				sys.exit(0)
			}
		}
	}
}

