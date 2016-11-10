/**
 * 
 */
package at.dhx.adempiere.ersync.event;

import java.util.Hashtable;
import java.util.Properties;

import org.adempiere.base.event.AbstractEventHandler;
import org.adempiere.base.event.IEventTopics;
import org.compiere.model.PO;
import org.compiere.model.Query;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Trx;
import org.osgi.service.event.Event;

import at.dhx.adempiere.ersync.model.I_I_Auto_Movement;
import at.dhx.adempiere.ersync.model.I_I_POS_Order;
import at.dhx.adempiere.ersync.model.I_I_Web_Order;

/**
 * @author dhx
 *
 */
public class CorrectImportEventHandler extends AbstractEventHandler {

	private Hashtable<String,String> htables;

	public CorrectImportEventHandler() {
		super();
		// All the products in these tables are to be synchronized when one of
		// them changes either the M_Product_ID or the ProductValue/UPC (
		// using UPC when its available and ProductValue otherwise)
		htables = new Hashtable<String,String>();
		htables.put(I_I_Web_Order.Table_Name, I_I_Web_Order.COLUMNNAME_UPC);
		htables.put(I_I_POS_Order.Table_Name, I_I_POS_Order.COLUMNNAME_UPC);
		htables.put(I_I_Auto_Movement.Table_Name, I_I_Auto_Movement.COLUMNNAME_ProductValue);
	}
	
	/**
	 * get the number of records matching
	 * @param ctx Properties
	 * @param tableName String
	 * @param whereClause String
	 * @param values Object[]
	 * @return unique record's ID in the table   
	 */
	private int getCount(Properties ctx,String tableName, String whereClause, Object[] values)
	{
		return new Query(ctx,tableName,whereClause,null).setClient_ID()
		.setParameters(values).count();
	}
	
	@Override
	protected void doHandleEvent(Event event) {
		if (event.getTopic().equals(IEventTopics.PO_AFTER_CHANGE)) {
			PO po = getPO(event);
			
			// First check if the table this event is triggered for is in the list (should not fail as
			// we only initialized this handler only for those tables)
			if (htables.containsKey(po.get_TableName())) {
				
				Trx trx = Trx.get(Trx.createTrxName("PropagateProductData"), true);

				try {
					Integer id = po.get_ID();
					Integer ad_client_id = po.getAD_Client_ID();
					Integer m_product_id = po.get_ValueAsInt("M_Product_ID");
					String documentno = po.get_ValueAsString("documentno");
					
					// depending if this table has UPC or not we take the upc or the productValue for propagation
					String productValue = po.get_ValueAsString(htables.get(po.get_TableName()));
					// now we iterate over all the tables to sync
					for (String mtable : htables.keySet()) {
						// and if it is not the table the change originates from
						if (!mtable.toLowerCase().equals(po.get_TableName().toLowerCase())) {
							// we try to find a single entry in this target table with the same primary record id
							if (getCount(Env.getCtx(), mtable, mtable + "_id = ?", new Object[]{id}) == 1) {
	
								StringBuilder sql = new StringBuilder("UPDATE ")
										.append(mtable)
										.append(" SET ");
								
								StringBuilder scond = new StringBuilder(" WHERE ")
										.append(mtable).append("_id = ? ")
										.append("AND AD_Client_ID = ? ")
										.append("AND (I_IsImported<>'Y' OR I_IsImported IS NULL) ")
										.append("AND (documentno = ?) ");
								
								// in any case we update the product value
								StringBuilder sqlSetProductValue = new StringBuilder(sql.toString())
										.append(htables.get(mtable))
										.append(" = ? ")
										.append(scond.toString())
										.append("AND ")
										.append(htables.get(mtable))
										.append(" != ? ");
								
								Object[] paramsSetProductValue = new Object[]{productValue, id, ad_client_id, documentno, productValue};
								int nval = DB.executeUpdate(sqlSetProductValue.toString(), paramsSetProductValue, false, trx.getTrxName());
								if (nval > 0) {
									System.out.println("Updated product value in related import table '" + mtable + "'"
											+ " in document '" + documentno + "' record " + id + ": "
											+ "set " + htables.get(mtable) + ": " + productValue);
								}
								
								// and the product_id only when its set in the source table
								if(m_product_id > 0) {
									StringBuilder sqlSetProductId = new StringBuilder(sql.toString())
										.append("M_Product_ID = ? ")
										.append(scond.toString())
										.append("AND ")
										.append("M_Product_ID != ? ");
									Object[] paramsSetProductId = new Object[]{m_product_id, id, ad_client_id, documentno, m_product_id};
									int nid = DB.executeUpdate(sqlSetProductId.toString(), paramsSetProductId, false, trx.getTrxName());
									if (nid > 0) {
										System.out.println("Updated product id in related import table '" + mtable + "'"
												+ " in document '" + documentno + "' record " + id + ": "
												+ "set m_product_id: " + m_product_id);
									}
								}
							}
						}
					}
					trx.commit();
				} catch (Exception e) {
					trx.rollback();
					throw e;
				} finally {
					trx.close();
				}
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see org.adempiere.base.event.AbstractEventHandler#initialize()
	 */
	@Override
	protected void initialize() {
		for(String tablename:htables.keySet()) {
			registerTableEvent(IEventTopics.PO_AFTER_CHANGE, tablename);
		}
	}

}
