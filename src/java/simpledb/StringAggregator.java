package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<Field,Integer> count;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if(what!=Op.COUNT) {
    		throw new IllegalArgumentException("String Agg Op is not equle to COUNT");
    	}
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	this.afield=afield;
    	this.what=what;
    	count=new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field tmp;
    	if(gbfield==-1) {
    		tmp=new IntField(-1);
    	}else {
    		tmp=tup.getField(gbfield);
    		if(tmp.getType()!=gbfieldtype)
    			throw new IllegalArgumentException("Given Tuple is error tuple");
    	}
    	
    	if(count.containsKey(tmp))
			count.put(tmp,count.get(tmp)+1);
		else
			count.put(tmp,1);
    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregatorIterator(count, gbfieldtype);
    }

    public class StringAggregatorIterator implements OpIterator{

    	private HashMap<Field,Integer> count;
        private Iterator<Field> iter;
        private TupleDesc td;
        private Boolean isGroup;
        
    	public StringAggregatorIterator(HashMap<Field,Integer> count,Type gtype) {
			// TODO Auto-generated constructor stub
    		this.count=count;
    		iter=null;
    		isGroup=(gtype!=null);
    		Type type[];
        	String name[];
        	if(isGroup) {
        		type=new Type[] {gtype,Type.INT_TYPE};
        		name=new String[]{"",""};
        	}else {
        		type=new Type[] {Type.INT_TYPE};
        		name=new String[]{""};
        	}
        	td=new TupleDesc(type,name);
		}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			iter=count.keySet().iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if(iter==null)
				return false;
			return iter.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			if(iter==null)
				return null;
			Field fd=iter.next();
			if(fd==null)
				return null;
			Tuple res=new Tuple(td);
			if(isGroup) {
				res.setField(0, fd);
				res.setField(1, new IntField(count.get(fd)));
			}else {
				res.setField(0, new IntField(count.get(fd)));
			}
			//System.out.println(res.toString());
			return res;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			iter=count.keySet().iterator();
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return td;
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			iter=null;
		}
    	
    }
}
