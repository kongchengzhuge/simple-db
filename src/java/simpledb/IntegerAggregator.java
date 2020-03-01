package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<Field,Integer> group;
    private HashMap<Field,Integer> count;
    
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	this.afield=afield;
    	this.what=what;
    	group=new HashMap<Field, Integer>();
    	count=new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
    	
    	int value=((IntField)tup.getField(afield)).getValue();
    	if(!group.containsKey(tmp)) {
    		group.put(tmp, value);
    		return;
    	}
    	switch(what) {
    	case MIN:
    		group.put(tmp, Math.min(group.get(tmp), value));
    		break;
    	case MAX:
    		group.put(tmp, Math.max(group.get(tmp), value));
    		break;
    	case AVG:
    	case SUM:
    		group.put(tmp, group.get(tmp)+value);
    		break;
    	case COUNT:
    		return;
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntegerAggregatorIterator(what==Op.COUNT,what==Op.AVG, group, count, gbfieldtype);
    }
    
    public class IntegerAggregatorIterator implements OpIterator{

    	private Boolean isGroup;
        private Boolean isAvg;
        private Boolean isCount;
        private HashMap<Field,Integer> group;
        private HashMap<Field,Integer> count;
        private Iterator<Field> iter;
        private TupleDesc td;
    	
        public IntegerAggregatorIterator(Boolean isCount,Boolean isAvg,HashMap<Field,Integer> group,
        		HashMap<Field,Integer> count,Type gtype) {
			// TODO Auto-generated constructor stub
        	this.isGroup= (gtype!=null);
        	this.isCount=isCount;
        	this.count=count;
        	this.group=group;
        	this.isAvg=isAvg;
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
			iter=group.keySet().iterator();
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
			if(isAvg) {
				int sum=group.get(fd);
				int co=count.get(fd);
				if(isGroup) {
					res.setField(1, new IntField(sum/co));
					res.setField(0, fd);
				}
				else 
					res.setField(0, new IntField(sum/co));
			}else if(isCount) {
				if(isGroup) {
					res.setField(1, new IntField(count.get(fd)));
					res.setField(0, fd);
				}
				else 
					res.setField(0, new IntField(count.get(fd)));
			}else {
				if(isGroup) {
					res.setField(1, new IntField(group.get(fd)));
					res.setField(0, fd);
				}
				else 
					res.setField(0, new IntField(group.get(fd)));
			}
			return res;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			iter=group.keySet().iterator();
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
