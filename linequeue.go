package linequeue		//file based line queue with lines counter

import(
	"io"
	"os"
    "bytes"  
    "encoding/binary" 
)

const SIZEOFCURSOR	int = 8
const SIZEOFCOUNTER int = 4
const SIZEOFHEADER	int = 20

type Linequeue struct {
	autoshrink bool
	linesize int
	lines uint32
	afile *os.File
}

func isExists( path string ) bool {
    _,err := os.Stat( path )
    if err == nil {
        return true
    }
    return os.IsExist( err )	//os.IsNotExist(err)?
}

func readCounter( afile *os.File ) uint32 {
	var lines uint32 = 0
	if afile!=nil {
		b := make( []byte, SIZEOFCOUNTER )
		n,err := afile.ReadAt( b,int64(SIZEOFCURSOR*2) )
		if err==nil && n==SIZEOFCOUNTER {	//byte2uint32
    		buf := bytes.NewBuffer( b ) 
	    	binary.Read(buf, binary.BigEndian, &lines)
		} 
	}
	return lines
}

func writeCounter( afile *os.File,lines uint32 ) bool{
	flag := false
	if afile!=nil {
		buf := bytes.NewBuffer( []byte{} )	//uint32 to byte
    	binary.Write( buf, binary.BigEndian, lines )
		_,err := afile.WriteAt( buf.Bytes(), int64(SIZEOFCURSOR*2) )
		flag = (err==nil)
	}
	return flag
} 

func readCursor( afile *os.File ) int64 {
	var pos int64 = 0
	if afile!=nil {
		b := make( []byte, SIZEOFCURSOR )
		n,err := afile.ReadAt( b,0 )
		if err==nil && n==SIZEOFCURSOR {	//byte2int64
    		buf := bytes.NewBuffer( b ) 
	    	binary.Read(buf, binary.BigEndian, &pos)
		} 
	}
	return pos
}

func writeCursor( afile *os.File,pos int64 ) bool{
	flag := false
	if afile!=nil {
		buf := bytes.NewBuffer( []byte{} )	//int64 to byte
    	binary.Write( buf, binary.BigEndian, pos )
		_,err := afile.WriteAt( buf.Bytes(),0 )
		flag = (err==nil)
	}
	return flag
}

func readPrevious( afile *os.File ) int64 {
	pos := int64(SIZEOFCURSOR) 
	if afile!=nil {
		b := make( []byte, SIZEOFCURSOR )
		n,err := afile.ReadAt( b,int64(SIZEOFCURSOR) )
		if err==nil && n==SIZEOFCURSOR {	//byte2int64
    		buf := bytes.NewBuffer( b )
	    	binary.Read(buf, binary.BigEndian, &pos)
		} 
	}
	return pos
}

func writePrevious( afile *os.File,pos int64 ) bool{
	flag := false
	if afile!=nil {
		buf := bytes.NewBuffer( []byte{} )	//int64 to byte
    	binary.Write( buf, binary.BigEndian, pos )
		_,err := afile.WriteAt( buf.Bytes(),int64(SIZEOFCURSOR) )
		flag = (err==nil)
	}
	return flag
}

func (lq *Linequeue)Empty() {
	if lq.afile!=nil {
		lq.afile.Truncate( int64(SIZEOFHEADER) )
		writeCursor( lq.afile,int64(SIZEOFHEADER) )
		writePrevious( lq.afile,int64(SIZEOFHEADER) )
		writeCounter( lq.afile,uint32(0) )
		lq.lines = 0
	}
}

func (lq *Linequeue)GetLines() uint32{
	return lq.lines
}

func (lq *Linequeue)PutInto(ln string) (nbytes int,lines uint32){
	nbytes = 0
	if lq.afile!=nil {
		lq.afile.Seek( 0,2 )	//end of file
		nbytes,_ = lq.afile.WriteString( ln+"\n" )
		if nbytes>0 {
			lq.lines ++
			writeCounter(lq.afile,lq.lines)
		}
	}
	return nbytes,lq.lines
}

func (lq *Linequeue)RollBackOneStep() (flag bool,pos int64) { //only one step
	flag = false
	pos = readPrevious( lq.afile )
	if pos<readCursor( lq.afile ) {
		writeCursor( lq.afile,pos )
		flag = true
	}
	return flag,pos
}

func (lq *Linequeue)TakeOut() (string,int){
	ln := ""
	sz := 0
	b := make( []byte,lq.linesize )
	if lq.afile!=nil {
		pos := readCursor( lq.afile )
		if pos>0 {
			o,err := lq.afile.Seek( pos,0 )
			if err==nil && o==pos {
				for {
					n,err := lq.afile.Read(b)
					if err==nil {
						if n<lq.linesize {	b = b[:n] }
						i := bytes.IndexByte( b,'\n' )
						if i>=0 {
							ln += string( b[:i] )
							o += int64(i)
							break
						}else{
							ln += string( b )		
							o += int64(n)
						}
					}else if err==io.EOF {
						break
					}
				}
			}
			if o==pos {		//Not forward
				if lq.autoshrink {
					fi,s_err := lq.afile.Stat()
					if s_err==nil {
						if fi.Size() == o {
							lq.afile.Truncate( int64(SIZEOFHEADER) )
							writeCursor( lq.afile,int64(SIZEOFHEADER) )
							writePrevious( lq.afile,int64(SIZEOFHEADER) )
							writeCounter( lq.afile,uint32(0) )
							lq.lines = 0
						}
					}
				}				
			}else{
				writeCursor( lq.afile,o+1 )
				writePrevious( lq.afile,pos )
			}
			sz = int(o-pos)
		}
	}
	return ln,sz
}

func SetFile( fname string,lnsize int,autoshrink bool ) (*Linequeue,bool){
	flag := false

	var err error = nil
	afile, err := os.OpenFile(fname, os.O_RDWR | os.O_CREATE, 0x644)
	if err == nil {
		fi,s_err := afile.Stat()
		if s_err==nil {
			sz := fi.Size()
			if sz==0 {
				writeCursor( afile,int64(SIZEOFHEADER) )	//cur pos
				writePrevious( afile,int64(SIZEOFHEADER) )	//pre pos
				writeCounter( afile,uint32(0) )
			}
			flag = true
		}
	}
	lq := &Linequeue{
		afile:		afile,
		linesize:	lnsize,
		lines:		readCounter( afile ),
		autoshrink:	autoshrink,
	}
	return lq,flag
}

func (lq *Linequeue)CloseFile() {
	if lq.afile!=nil {
		lq.afile.Close()
	}
}