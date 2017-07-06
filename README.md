# linequeue
file based line queue with lines counter

package main

import(

	"fmt"	
	"linequeue"
	
)

func main(){

	fmt.Println("line queue")
	lq,flag := linequeue.SetFile( "c:\\data\\lnqueue",256,false )
	fmt.Println( flag )
	if flag {
		lq.PutInto( "abcdefghi" )
		lq.PutInto( "123456789" )
		lq.PutInto( "ABCDEF" )
		for{
			ln,n := lq.TakeOut()
			fmt.Println( ln,n )
			if n==0 { break }
		}
		fmt.Println("-----")
		lq.RollBackOneStep()
		fmt.Println("rollback")
		ln,n := lq.TakeOut()
		fmt.Println( ln,n )
		fmt.Println( lq.GetLines() )
		lq.CloseFile()
	}
}


result:

line queue

true

abcdefghi 9

123456789 9

ABCDEF 6

0

-----

rollback

ABCDEF 6

6
