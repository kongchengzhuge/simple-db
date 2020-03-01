package simpledb;

import java.io.*;
import java.nio.channels.WritePendingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.prism.paint.Stop;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    //private ConcurrentHashMap<PageId, Page> cache;
    private int numPages;
    private PageNode header,tail;
    private ConcurrentHashMap<PageId, PageNode> cache;
    private LockManager pageLockman;
    private HashMap<TransactionId, ArrayList<PageId>> trandirtypage;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages=numPages;
    	cache=new ConcurrentHashMap<PageId, PageNode>();
    	header=new PageNode(null);
    	tail=new PageNode(null);
    	header.next=tail;
    	tail.pre=header;
    	this.pageLockman=new LockManager();
    	trandirtypage=new HashMap<TransactionId, ArrayList<PageId>>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	int thisType=1;
    	if(perm==Permissions.READ_WRITE)
    		thisType=0;
    	long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
    	while(!pageLockman.acquire(pid, tid, thisType)) {
    		 long now = System.currentTimeMillis();
             if(now-start > timeout){
                 // TransactionAbortedException means detect a deadlock
                 // after upper caller catch TransactionAbortedException
                 // will call transactionComplete to abort this transition
                 // give someone else a chance: abort the transaction
                 throw new TransactionAbortedException();
             }
    	}
    	//System.out.println("time");
    	if(!cache.containsKey(pid)) {
    		DbFile dbfile=Database.getCatalog().getDatabaseFile(pid.getTableId());
    		Page pg=dbfile.readPage(pid);
    		PageNode pagenode=new PageNode(pg,null,null);
    		addFirst(pagenode);
    		cache.put(pid, pagenode);
    		evictPage();
    		return pg;
    	}
    	PageNode pagenode=cache.get(pid);
    	moveTohead(pagenode);
        return pagenode.page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	pageLockman.release(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.pageLockman.holdslock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if(commit) {
    		flushPages(tid);
    	}else {
    		restorePages(tid);
    	}
    	
    	for(PageId pid:cache.keySet()) {
    		if(holdsLock(tid, pid)) {
    			releasePage(tid, pid);
    		}
    	}
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile dbfile=Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> res=dbfile.insertTuple(tid, t);
    	for(Page pg:res) {
    		pg.markDirty(true, tid);
    		PageNode pagenode=cache.get(pg.getId());
    		moveTohead(pagenode);
    		cache.put(pg.getId(), pagenode);
    		evictPage();
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile dbfile=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	ArrayList<Page> res=dbfile.deleteTuple(tid, t);
    	for(Page pg:res) {
    		pg.markDirty(true, tid);
    		PageNode pagenode=new PageNode(pg);
    		moveTohead(pagenode);
    		cache.put(pg.getId(), pagenode);
    		evictPage();
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	Iterator<PageId> pids=cache.keySet().iterator();
    	while(pids.hasNext()) {
    		flushPage(pids.next());
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	if(!cache.containsKey(pid))
    		return;
    	PageNode pgno=cache.get(pid);
    	remove(pgno);
    	cache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Page pg=cache.get(pid).page;
    	if(pg.isDirty()!=null) {
    		Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pg);
    		pg.markDirty(false, null);
    	}
    	
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	for(PageId pid:cache.keySet()) {
    		Page pg=cache.get(pid).page;
    		if(pg.isDirty()==tid) {
    			flushPage(pid);
    		}
    	}
    }
    
    private synchronized  void restorePage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	//System.out.println("restore Page id is "+pid.toString());
    	PageNode pgnode=cache.get(pid);
    	if(pgnode.page.isDirty()!=null) {
    		Page pg=Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    		//byte data[]=pg.getPageData();
    		//System.out.println(Arrays.toString(data));
    		pgnode.page=pg;
    		cache.put(pid, pgnode);
    		pg.markDirty(false, null);
    	}
    	
    }
    
    public synchronized  void restorePages(TransactionId tid) throws IOException  {
        // some code goes here
        // not necessary for lab1|lab2
    	for(PageId pid:cache.keySet()) {
    		Page pg=cache.get(pid).page;
    		if(pg.isDirty()==tid) {
    			restorePage(pid);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	if(cache.size()<=numPages) 
    		return;
    	PageNode evict=evictNoDirty(); //不把dirty page移出
    	cache.remove(evict.page.getId());
    }

    //lru有关函数如下
    //Lru cache Node
    public class PageNode{
    	public PageNode next;
    	public PageNode pre;
    	public Page page;
    	
    	PageNode(Page page){
    		this.page=page;
    		next=null;
    		pre=null;
    	}
    	
    	PageNode(Page page,PageNode next,PageNode pre){
    		this.page=page;
    		this.next=next;
    		this.pre=pre;
    	}
    }
    
    public void printLruNode() {
    	PageNode tmp=header;
    	while(tmp.next!=tail) {
    		tmp=tmp.next;
    		System.out.print(tmp.page.getId().toString());
    		if(tmp.next!=tail)
    			System.out.print("->");
    	}
    	System.out.print("\n");
    }
    
    public void moveTohead(PageNode pagenode) {
    	this.remove(pagenode);
    	this.addFirst(pagenode);
    }
    
    public void remove(PageNode pagenode) {
    	pagenode.next.pre=pagenode.pre;
    	pagenode.pre.next=pagenode.next;
    }
    
    public PageNode evictNoDirty() throws DbException {
    	//System.out.println("evict");
    	PageNode old=tail.pre;
    	while(old.page.isDirty()!=null) {
    		old=old.pre;
    		if(old==header) {
    			throw new DbException("no thing can be exicted");
    		}
    	}
    	this.remove(old);
    	return old;
    }
    
    public PageNode popTail() {
    	PageNode old=tail.pre;
    	this.remove(old);
    	return old;
    }
    
    public void addFirst(PageNode pagenode) {
    	pagenode.next=header.next;
    	pagenode.next.pre=pagenode;
    	pagenode.pre=header;
    	header.next=pagenode;
    }
    
    public boolean empty() {
    	return header.next==tail;
    }
    
    
    
    //lock有关函数如下
    public class Lock{
    	int lockType;
    	TransactionId tid;
    	
    	Lock(int lockType,TransactionId tid){
    		this.lockType=lockType;
    		this.tid=tid;
    	}
    }
    
    public class LockManager{
    	ConcurrentHashMap<PageId, Vector<Lock>> p2locks;
    	
    	public LockManager() {
			// TODO Auto-generated constructor stub
    		p2locks=new ConcurrentHashMap<>();
		}
    	
    	public synchronized Boolean acquire(PageId pid,TransactionId tid,int lockType) {
    		if((!p2locks.containsKey(pid))||p2locks.get(pid)==null) {
    			Lock lc=new Lock(lockType, tid);
    			Vector<Lock> ve=new Vector<BufferPool.Lock>();
    			ve.add(lc);
    			p2locks.put(pid, ve);
    			return true;
    		}
    		
    		//System.out.println(2);
    		Vector<Lock> ve=p2locks.get(pid);
    		int thisType=1;
    		if(ve.size()==1&&ve.get(0).lockType==0)
    			thisType=0;
    		
    		//遍历ve，判断是否有相同事务已经得到锁
    		for(Lock lc:ve) {
    			if(lc.tid.equals(tid)) {
    				if(thisType==0||1==lockType)
    					return true;
    				else {
    					if(ve.size()==1) {
    						lc.lockType=0;
    						return true;
    					}
    					return false;
    				}
    			}
    		}
    		
    		//之前没有相同事务得到锁，且page有锁，如果有一个是写锁，return false
    		if(lockType==0||thisType==0)
    			return false;
    		
    		//都是读锁，可以进去
    		Lock newSlock=new Lock(1,tid);
    		ve.add(newSlock);
    		return true;
    	}
    	
    	public synchronized void release(PageId pid,TransactionId tid) {
    		if(!p2locks.containsKey(pid)) {
    			return;
    		}
    		Vector<Lock> ve=p2locks.get(pid);
    		for(int i=0;i<ve.size();++i) {
    			Lock lc=ve.get(i);
    			
    			if(lc.tid.equals(tid)) {
    				ve.remove(lc);
    				if(ve.isEmpty()) {
    					p2locks.remove(pid);
    				}
    				return;
    			}
    		}
    	}
    	
    	public synchronized Boolean holdslock(PageId pid,TransactionId tid) {
    		if(!p2locks.containsKey(pid)) {
    			return false;
    		}
    		Vector<Lock> ve=p2locks.get(pid);
    		for(Lock lc:ve) {
    			if(lc.tid.equals(tid)) {
    				return true;
    			}
    		}
    		return false;
    	}
    }
}
