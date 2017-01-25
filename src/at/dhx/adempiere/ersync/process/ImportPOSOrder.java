/******************************************************************************
 * Product: Adempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 1999-2006 ComPiere, Inc. All Rights Reserved.                *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 * For the text or an alternative of this public license, you may reach us    *
 * ComPiere, Inc., 2620 Augustine Dr. #245, Santa Clara, CA 95054, USA        *
 * or via info@compiere.org or http://www.compiere.org/license.html           *
 *****************************************************************************/
package at.dhx.adempiere.ersync.process;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;

import org.compiere.model.MStorageOnHand;
import org.compiere.util.DB;

import at.dhx.adempiere.ersync.model.I_I_Extended_Order;
import at.dhx.adempiere.ersync.model.I_I_POS_Order;
import at.dhx.adempiere.ersync.model.X_M_Product;

/**
 *	Import Order from I_POS_Order
 *
 *  @author     Daniel Haag (dhx.at)
 */
public class ImportPOSOrder extends ImportExtendedOrder
{

	/**
	 * The unallocatedQty hashmap is used to record the quantities not yet
	 * allocated to a pos order line to prevent
	 * double allocating a quantity on hand.
	 */
	private HashMap<List<Integer>,BigDecimal> unallocatedQty = new HashMap<List<Integer>,BigDecimal>();

	private void checkWarehouseQtyOnHand(I_I_POS_Order iorder) {
		Integer warehouse_id = iorder.getM_Warehouse_ID();
		Integer product_id = iorder.getM_Product_ID();
		
		BigDecimal qtyAvailable;
		
		if (warehouse_id != null && product_id != null) {
			List<Integer> wpkey = Arrays.asList(warehouse_id,product_id);
			if (!unallocatedQty.containsKey(wpkey)) {
				qtyAvailable = MStorageOnHand.getQtyOnHand(product_id, warehouse_id, 0, get_TrxName());
				unallocatedQty.put(wpkey, qtyAvailable);
			} else {
				qtyAvailable = unallocatedQty.get(wpkey);
			}
			if (qtyAvailable.compareTo(iorder.getQtyOrdered()) >= 0) {
				// there is still enough qty available -> allocate it
				unallocatedQty.put(wpkey, qtyAvailable.subtract(iorder.getQtyOrdered()));
			} else {
				// there is not enough qty available -> set error on this line
				StringBuilder sql = new StringBuilder("UPDATE ")
						.append(getM_TableName())
						.append(" SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Lagerstand fuer POS Auftrag fehlt, ' ")
						.append("WHERE ")
						.append(getM_TableName()).append("_ID=")
						.append(iorder.getI_POS_Order_ID())
						.append(" AND I_IsImported<>'Y'")
						.append(" AND AD_Client_ID=")
						.append(m_AD_Client_ID);

				DB.executeUpdate(sql.toString(), get_TrxName());
			}
		}
	}
	
	protected void preflight() {

		StringBuilder sql = new StringBuilder("SELECT ")
				.append("i.* FROM ")
				.append(getM_TableName())
				.append(" AS i")
				.append(" INNER JOIN M_Product p ON p.M_Product_ID = i.M_Product_ID")
				.append(" WHERE i.I_IsImported='N'")
				.append(" AND p.ProductType='" + X_M_Product.PRODUCTTYPE_Item + "'")
				.append(" AND i.documentno NOT IN (SELECT DISTINCT documentno FROM ")
				.append(getM_TableName())
				.append(" WHERE I_IsImported='E')")
				.append(" AND i.AD_Client_ID=")
				.append(m_AD_Client_ID)
				.append(" ORDER BY i.C_BPartner_ID, i.BillTo_ID, i.C_BPartner_Location_ID, ")
				.append("i.").append(getM_TableName()).append("_ID");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = DB.prepareStatement(sql.toString(), get_TrxName());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				I_I_Extended_Order imp = this.getI_Extended_Order(getCtx(), rs,	get_TrxName());
				checkWarehouseQtyOnHand((I_I_POS_Order)imp);
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Order - " + sql.toString(), e);
		} finally {
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}
	}
}	//	ImportPOSOrder
