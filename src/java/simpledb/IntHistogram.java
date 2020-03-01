package simpledb;

import java.util.ArrayList;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int buckets;
	private int min;
	private int max;
	private double width;
	private int ntups;
	private ArrayList<Integer> bucket;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets=buckets;
    	this.min=min;
    	this.max=max;
    	this.ntups=0;
    	this.width=(max-min+1)*1.0/buckets;
    	bucket=new ArrayList<Integer>(buckets);
    	for(int i=0;i<buckets;++i)
    		bucket.add(0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	if(v<min|v>max)
    		return;
    	int t=bucket.get(index(v));
    	bucket.set(index(v), ++t);
    	ntups++;
    }
    
    public int index(int v) {
    	//System.out.println(String.format("max:%d,min:%d,width:%f,buckets:%d", max,min,width,buckets));
    	return (int)((v-min)/width);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	/*
    	double res=0;
    	switch (op) {
		case EQUALS:
			return estimateEqule(v);
		case GREATER_THAN:
			return estimateGreater(v);
		case GREATER_THAN_OR_EQ:
			res+=estimateEqule(v);
			res+=estimateGreater(v);
			return res;
		case NOT_EQUALS:
			return 1.0-estimateEqule(v);
		case LESS_THAN:
			res+=estimateEqule(v);
			res+=estimateGreater(v);
			return 1.0-res;
		case LESS_THAN_OR_EQ:
			return estimateSelectivity(Op.LESS_THAN, v+1);
		case LIKE:
			System.out.println("Like is not implement");
			return 1.0;
		default:
			System.out.println("aother case");
			return 1.0;
		}
		*/
    	if(op.equals(Predicate.Op.LESS_THAN)){
            if(v <= min) return 0.0;
            if(v >= max) return 1.0;
            final int index = index(v);
            double cnt = 0;
            for(int i=0;i<index;++i){
                cnt += bucket.get(i);
            }
            cnt += bucket.get(index)/width*(v-index*width-min);
            return cnt/ntups;
        }
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v+1);
        }
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            return 1-estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }
        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v-1);
        }
        if (op.equals(Predicate.Op.EQUALS)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) -
                    estimateSelectivity(Predicate.Op.LESS_THAN, v);
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;

    }
    
    public double estimateGreater(int v) { 	
    	if(v<min)
    		return 1.0;
    	if(v>max)
    		return 0.0;
    	int in=index(v);
    	//System.out.println(in);
    	double res=0;
    	for(int i=in+1;i<buckets;++i)
    		res+=bucket.get(i);
    	double w_b=min+(in+1)*width-v;
    	res+=bucket.get(in)/w_b;
    	return res/ntups;
    }
    
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("%d IntHistogram with max %d min %d",buckets,max,min); 
    }
}
