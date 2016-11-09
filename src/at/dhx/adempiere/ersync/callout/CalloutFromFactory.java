/**
 * 
 */
package at.dhx.adempiere.ersync.callout;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.adempiere.base.IColumnCallout;
import org.compiere.model.GridField;
import org.compiere.model.GridTab;
import org.compiere.model.Query;
import org.compiere.util.DB;
import org.compiere.util.Env;

import at.dhx.adempiere.ersync.model.I_I_Auto_Movement;
import at.dhx.adempiere.ersync.model.X_I_Auto_Movement;
import at.dhx.adempiere.ersync.model.X_I_POS_Order;
import at.dhx.adempiere.ersync.model.X_I_Web_Order;

/**
 * @author dhx
 *
 */
public class CalloutFromFactory implements IColumnCallout {

	
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
	
	private void setTableField(Properties ctx, String targetTable, Object value, String field) {
		/*
		switch (field) {
		case I_I_Auto_Movement.COLUMNNAME_M_Product_ID:
			Integer M_Product_ID = (Integer)value;
			if (i_web_order != null && i_web_order.isActive() && !i_web_order.isI_IsImported()) {
				if (M_Product_ID > 0 && i_web_order.getM_Product_ID() != M_Product_ID) {
					i_web_order.setM_Product_ID(M_Product_ID);
					if (i_web_order.save()) {
						System.out.println("Updated related web order (" + i_web_order.getDocumentNo() + ") m_product_id: " + M_Product_ID.toString());
					} else {
						System.out.println("Could not update related web order (" + i_web_order.getDocumentNo() + ") m_product_id: " + M_Product_ID.toString());
					}					
				}
			}
			if (i_pos_order != null && i_pos_order.isActive() && !i_pos_order.isI_IsImported()) {
				if (M_Product_ID > 0 && i_pos_order.getM_Product_ID() != M_Product_ID) {
					i_pos_order.setM_Product_ID(M_Product_ID);
					if (i_pos_order.save()) {
						System.out.println("Updated related POS order (" + i_pos_order.getDocumentNo() + ") m_product_id: " + M_Product_ID.toString());
					} else {
						System.out.println("Could not update related POS order (" + i_pos_order.getDocumentNo() + ") m_product_id: " + M_Product_ID.toString());
					}					
				}
			}
			break;
		case I_I_Auto_Movement.COLUMNNAME_ProductValue:
			String ProductValue = (String)value;
			if (i_web_order != null && i_web_order.isActive() && !i_web_order.isI_IsImported()) {
				if (!ProductValue.isEmpty() && !i_web_order.getProductValue().equals(ProductValue)) {
					i_web_order.setProductValue(ProductValue);
					if (i_web_order.save()) {
						System.out.println("Set product value of related web order to: " + value);
					} else {
						System.out.println("Could not set product value of related web order to: " + value);
					}					
				}
			}
			if (i_pos_order != null && i_pos_order.isActive() && !i_pos_order.isI_IsImported()) {
				if (!ProductValue.isEmpty() && !i_pos_order.getProductValue().equals(ProductValue)) {
					i_pos_order.setProductValue(ProductValue);
					if (i_pos_order.save()) {
						System.out.println("Set product value of related POS order to: " + value);
					} else {
						System.out.println("Could not set product value of related POS order to: " + value);
					}					
				}
			}
			break;
		}*/
		
	}
	
	/* (non-Javadoc)
	 * @see org.adempiere.base.IColumnCallout#start(java.util.Properties, int, org.compiere.model.GridTab, org.compiere.model.GridField, java.lang.Object, java.lang.Object)
	 */
	@Override
	public String start(Properties ctx, int WindowNo, GridTab mTab,
			GridField mField, Object value, Object oldValue) {
		System.out.println(ctx + " - " + WindowNo + " - " + mTab + " - " + mField + " - " + value + " - " + oldValue);

		//int AD_Client_ID = Env.getContextAsInt(ctx, WindowNo, mTab.getTabNo(), "AD_Client_ID");
		//Integer id = mTab.getRecord_ID();
		//int AD_Client_ID2 = (int)mTab.getValue("AD_Client_ID");
		/*
		List<String> mtables = new ArrayList<String>();
		mtables.add(X_I_Web_Order.Table_Name);
		mtables.add(X_I_POS_Order.Table_Name);
		mtables.add(X_I_Auto_Movement.Table_Name);
		
		if (mtables.contains(mTab.getTableName())) {
			for (String mtable : mtables) {
				if (mtable != mTab.getTableName()) {
					if (getCount(ctx, mtable, mtable + "_id = ?", new Object[]{id}) == 1) {
						StringBuilder sql = new StringBuilder("UPDATE ").append(mtable)
								.append(" SET ")
								.append(mField.getColumnName())
								.append(" = ? WHERE ")
								.append(mtable).append("_id = ? ")
								.append("AND AD_Client_ID = ? ")
								.append("AND (I_IsImported<>'Y' OR I_IsImported IS NULL)");
						Object[] params = new Object[]{value, id, new Integer(AD_Client_ID)};
						DB.executeUpdate(sql.toString(), params, false, null);
					}
				}
			}
		}
		*/
		/*
		X_I_Web_Order i_web_order = null;
		if (getCount(ctx,X_I_Web_Order.Table_Name,X_I_Web_Order.COLUMNNAME_I_Web_Order_ID + " = ?", new Object[]{id}) == 1) {
			i_web_order = new X_I_Web_Order(ctx, id, null);			
		}
		X_I_POS_Order i_pos_order = null;
		if (getCount(ctx,X_I_POS_Order.Table_Name,X_I_POS_Order.COLUMNNAME_I_POS_Order_ID + " = ?", new Object[]{id}) == 1) {
			i_pos_order = new X_I_POS_Order(ctx, id, null);			
		}
		*/
		return null;
	}

}
