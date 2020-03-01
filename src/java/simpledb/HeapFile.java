package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File f;
	private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	//System.out.println("origin f.size is :"+f.length());
    	this.f=f;
    	this.td=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        // some code goes here
    	//int tableid=pid.getTableId();
    	int pgno=pid.getPageNumber();
    	
    	RandomAccessFile rf=null;
    	
    	try {
			rf=new RandomAccessFile(f,"r");
			if((pgno+1)*BufferPool.getPageSize()>rf.length()) {
				rf.close();
				throw new IllegalArgumentException(String.format("read pgno %d but file has only %d pages", pgno,numPages()));
			}
			byte bytes[]=new byte[BufferPool.getPageSize()];
			rf.seek(pgno*BufferPool.getPageSize());
			int res=rf.read(bytes,0,BufferPool.getPageSize());
			if(res!=BufferPool.getPageSize())
				throw new IllegalArgumentException();
			HeapPageId id=new HeapPageId(pid.getTableId(), pid.getPageNumber());
			return new HeapPage(id, bytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				rf.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	//System.out.println("write page :"+page.getId().getPageNumber());
    	int pgno=page.getId().getPageNumber();
    	byte[] data=page.getPageData();
    	
    	RandomAccessFile rf=new RandomAccessFile(f, "rw");
    	if(pgno>numPages()) {
    		rf.close();
    		throw new IllegalArgumentException();
    	}
    	rf.seek(pgno*BufferPool.getPageSize());
    	rf.write(data, 0, BufferPool.getPageSize());
    	rf.close();
    } 

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	//System.out.println("file.size is :"+f.length());
        return (int)Math.floor(f.length()*1.0/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	// some code goes here
    	if(!t.getTupleDesc().equals(td))
    		throw  new DbException("tupeldesc is not fit");
        HeapPage pg=null;
        int i=0;
        while(i<numPages()) {
        	//System.out.println(numPages()+":"+i);
        	PageId pid=new HeapPageId(getId(), i);
        	pg=(HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	//Database.getBufferPool().printLruNode(); 
        	if(pg.getNumEmptySlots()>0) {
        		break;
        	}
        	Database.getBufferPool().releasePage(tid, pid); //检查完之后，释放锁
        	i++;
        }
        if(pg==null||pg.getNumEmptySlots()==0) {	//注意如果循环结束，pg不为空，但是没有空间
        	BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f,true));
            byte[] emptyData = HeapPage.createEmptyPageData();
            bw.write(emptyData);
            bw.close();
        	pg=(HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);//再读一次，得到锁
        	//System.out.println("pid is"+pg.getId().toString());
        	//Database.getBufferPool().printLruNode();
        }
        pg.insertTuple(t);
    	ArrayList<Page> res=new ArrayList<Page>();
    	res.add(pg);
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	if(!t.getTupleDesc().equals(td))
    		throw  new DbException("tupeldesc is not fit");
    	HeapPage pg=(HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        pg.deleteTuple(t);
        ArrayList<Page> res=new ArrayList<Page>();
    	res.add(pg);
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
    
    private static final class HeapFileIterator implements DbFileIterator{

    	private final HeapFile hpfile;
    	private final TransactionId tid;
    	private Iterator<Tuple> iter;
    	private int WhichPage;
    	
    	public HeapFileIterator(HeapFile hf,TransactionId td) {
			// TODO Auto-generated constructor stub
    		hpfile=hf;
    		tid=td;
    		iter=null;
		}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			WhichPage=0;
			iter=getPageTuples(WhichPage);
		}
		
		public Iterator<Tuple> getPageTuples(int WhichPage) throws TransactionAbortedException, DbException{
			//System.out.println("heapfile size is:"+hpfile.numPages());
			if(WhichPage>=0&&WhichPage<hpfile.numPages()) {
				PageId pid=new HeapPageId(hpfile.getId(), WhichPage);
				return ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
			}else
				throw new DbException(String.format("error in page %d", WhichPage));
			
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if(iter==null)
				return false;
			if(!iter.hasNext()) {
				if(WhichPage<(hpfile.numPages()-1)) {
					WhichPage++;
					iter=getPageTuples(WhichPage);
					return iter.hasNext();
				}
				return false;
			}
			return true;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			if(iter==null||!iter.hasNext())
				throw new NoSuchElementException();
			return iter.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			close();
			open();
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			iter=null;
		}
    	
    }

}

