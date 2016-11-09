/**
 * 
 */
package at.dhx.adempiere.ersync.event;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
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
import at.dhx.adempiere.ersync.model.X_I_Auto_Movement;
import at.dhx.adempiere.ersync.model.X_I_POS_Order;
import at.dhx.adempiere.ersync.model.X_I_Web_Order;

/**
 * @author dhx
 *
 */
public class CorrectImportEventHandler extends AbstractEventHandler {

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
			
			Integer id = po.get_ID();
			Integer m_product_id = po.get_ValueAsInt("M_Product_ID");
			
			Integer ad_client_id = po.getAD_Client_ID();
			
			Trx m_trx = Trx.get(Trx.createTrxName("PropagateProductData"), true);

			Hashtable<String,String> htables = new Hashtable<String,String>();
			htables.put(X_I_Web_Order.Table_Name, X_I_Web_Order.COLUMNNAME_UPC);
			htables.put(X_I_POS_Order.Table_Name, X_I_POS_Order.COLUMNNAME_UPC);
			htables.put(X_I_Auto_Movement.Table_Name, X_I_Auto_Movement.COLUMNNAME_ProductValue);
			
			if (htables.containsKey(po.get_TableName())) {
				String productValue = po.get_ValueAsString(htables.get(po.get_TableName()));
				for (String mtable : htables.keySet()) {
					if (!mtable.toLowerCase().equals(po.get_TableName().toLowerCase())) {
						if (getCount(Env.getCtx(), mtable, mtable + "_id = ?", new Object[]{id}) == 1) {

							Object[] params = null;

							StringBuilder sql = new StringBuilder("UPDATE ").append(mtable)
									.append(" SET ");
							
							if(m_product_id > 0) {
								sql.append("M_Product_ID = ?, ");
								params = new Object[]{m_product_id, productValue, id, ad_client_id};
							} else {
								params = new Object[]{productValue, id, ad_client_id};
							}
							
							sql.append(htables.get(mtable))
									.append(" = ? ")
									.append(" WHERE ")
									.append(mtable).append("_id = ? ")
									.append("AND AD_Client_ID = ? ")
									.append("AND (I_IsImported<>'Y' OR I_IsImported IS NULL)");
							int no = DB.executeUpdate(sql.toString(), params, false, m_trx.getTrxName());
							if (no > 0) {
								System.out.println("Updated product info in related import table '" + mtable + "': "
										+ "set M_Product_ID: " + m_product_id + ", " + htables.get(mtable) + ": " + productValue);
							}
						}
					}
				}
			}
			m_trx.commit();
		}
	}
	
	
	/* (non-Javadoc)
	 * @see org.adempiere.base.event.AbstractEventHandler#initialize()
	 */
	@Override
	protected void initialize() {
		registerTableEvent(IEventTopics.PO_AFTER_CHANGE, I_I_Auto_Movement.Table_Name);
		registerTableEvent(IEventTopics.PO_AFTER_CHANGE, I_I_POS_Order.Table_Name);
		registerTableEvent(IEventTopics.PO_AFTER_CHANGE, I_I_Web_Order.Table_Name);
	}

}
