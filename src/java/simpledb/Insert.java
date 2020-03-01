package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private Boolean isfirst;
    
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.t=t;
    	this.child=child;
    	this.tableId=tableId;
    	this.isfirst=true;
    	td=new TupleDesc(new Type[] {Type.INT_TYPE},new String[] {""});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	isfirst=true;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!isfirst)
        	return null;
        Tuple res=new Tuple(getTupleDesc());
    	BufferPool bp=Database.getBufferPool();
    	int count=0;
        while(child.hasNext()) {
        	Tuple tmp=child.next();
        	try {
				bp.insertTuple(t, tableId, tmp);
        	} catch (IOException e) {
				// TODO Auto-generated catch block
        		e.printStackTrace();
        		return null;
			}
        	count++;
        }
        res.setField(0, new IntField(count));
        isfirst=false;
        //System.out.println(count);
    	return res;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	child=children[0];
    }
}
