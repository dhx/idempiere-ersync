package at.dhx.adempiere.ersync.model;

import java.util.Properties;

public class MProduct extends org.compiere.model.MProduct implements I_M_Product {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1935270436472880334L;


	public MProduct(Properties ctx, int M_Product_ID, String trxName) {
		super(ctx, M_Product_ID, trxName);
	}

	/** Set On Order.
		@param IsOnOrder 
		Indicates that this record is on order
	 */
	public void setIsOnOrder (boolean IsOnOrder)
	{
		set_Value (COLUMNNAME_IsOnOrder, Boolean.valueOf(IsOnOrder));
	}

	/** Get On Order.
		@return Indicates that this record is on order
	  */
	public boolean isOnOrder () 
	{
		Object oo = get_Value(COLUMNNAME_IsOnOrder);
		if (oo != null) 
		{
			 if (oo instanceof Boolean) 
				 return ((Boolean)oo).booleanValue(); 
			return "Y".equals(oo);
		}
		return false;
	}

}
