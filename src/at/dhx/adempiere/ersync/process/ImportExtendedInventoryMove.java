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
 * Contributor: Victor Perez, www.e-evolution.com                             *
 *****************************************************************************/
package at.dhx.adempiere.ersync.process;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import org.compiere.model.I_M_Movement;
import org.compiere.model.MBPartner;
import org.compiere.model.MCampaign;
import org.compiere.model.MDocType;
import org.compiere.model.MLocator;
import org.compiere.model.MMovement;
import org.compiere.model.MMovementLine;
import org.compiere.model.MOrg;
import org.compiere.model.MProduct;
import org.compiere.model.MProject;
import org.compiere.model.MShipper;
import org.compiere.model.MStorageOnHand;
import org.compiere.model.MWarehouse;
import org.compiere.model.Query;
import org.compiere.model.X_M_Warehouse;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.Env;
import org.compiere.util.Msg;

import at.dhx.adempiere.ersync.model.X_I_Auto_Movement;



/**
 *	ImportExtendedInventoryMove is based on:
 *  Import Inventory Movement from I_M_Movement
 *
 * 	@author 	Alberto Juarez Caballero, alberto.juarez@e-evolution.com, www.e-evolution.com
 * 	@author 	victor.perez@e-evolution.com, www.e-evolution.com
 * 	@version 	$Id: ImportInventoryMovement.java,v 1.0
 */

public class ImportExtendedInventoryMove extends SvrProcess
{

	private boolean			m_DeleteOldImported = false;

	private boolean			m_IsImportOnlyNoErrors = true;
	
	private String			m_docAction = MMovement.DOCACTION_Prepare;
	
	private boolean 		isImported = false;
	
	private int 			imported = 0;
	
	private int 			notimported = 0;
	
	private List<String> idsPr = new ArrayList<String>();

	/**
	 *  Prepare - e.g., get Parameters.
	 */
	protected void prepare()
	{
		ProcessInfoParameter[] parameters = getParameter();
		for (ProcessInfoParameter para: parameters)
		{
			String name = para.getParameterName();
			if (para.getParameter() == null)
				;

			else if (name.equals("IsImportOnlyNoErrors"))
				m_IsImportOnlyNoErrors = "Y".equals(para.getParameter());
			else if (name.equals("DeleteOldImported"))
				m_DeleteOldImported = "Y".equals(para.getParameter());
			else if (name.equals("DocAction"))
				m_docAction = (String)para.getParameter();
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);			
		}
	}	//	prepare


	/**
	 *  Perform process.
	 *  @return Message
	 *  @throws Exception
	 */
	protected String doIt() throws java.lang.Exception
	{
		
//		Delete Old Imported
		if (m_DeleteOldImported)
		{
			int no = 0;
			for (X_I_Auto_Movement move : getRecords(true,false))
			{
			    move.deleteEx(true);
			    no++;
			}
			if (log.isLoggable(Level.FINE)) log.fine("Delete Old Impored =" + no);
		}
		
		fillIDValues();
		// all movement lines having insufficient quantity on hand in the source locator
		// we try to split up when there is another locator having qty on hand for the desired product
		splitInsufficientQOHMovements();
		importRecords();	
		return "Imported: " + imported + ", Not imported: " + notimported;
	}	//	doIt
	
	
	/**
	 * import records using I_M_Movement table
	 */
	
	private void importRecords()
	{
		isImported = false;
		
		for(X_I_Auto_Movement imove : getRecords(false,m_IsImportOnlyNoErrors))
		{
			MMovement mov = importMInventoryMove(imove);			
			if(mov!= null)
			{    
				isImported = importMInventoryMoveLine(mov,imove);
			}	
			else
			{    
				isImported = false;
			}	
			
			if(isImported)
			{
				imove.setM_Movement_ID(mov.getM_Movement_ID());
				imove.setI_IsImported(true);
				imove.setProcessed(true);
				imove.saveEx();
				imported++;
				
				//mov.processIt(m_docAction);
				addForProcess(mov.getM_Movement_ID());
				mov.saveEx();
			}
			else
			{
				imove.setI_IsImported(false);
				imove.setProcessed(true);
				imove.saveEx();
				notimported++;
			}
		}
		processAll();
	}
	
	private void addForProcess(int id)
	{
		String ids = String.valueOf(id);
		boolean enc = false;
		for(String idx : idsPr)
		{
			if(idx.equals(ids))
				enc=true;
		}
		if(!enc)
			idsPr.add(ids);
	}
	
	private void processAll()
	{
		for(String idx : idsPr)
		{
			int id = Integer.parseInt(idx);
			MMovement move = new MMovement(Env.getCtx(), id, get_TrxName());
			move.processIt(m_docAction);
			move.saveEx();
		}
	}
	
	/**
	 * Import Inventory Move Line using X_I_M_Movement table
	 * @param move MMovement
	 * @param imove X_I_M_Movement
	 * @return isImported
	 */
	private boolean importMInventoryMoveLine(MMovement move, X_I_Auto_Movement imove)
	{
		isImported = false;

		MMovementLine moveLine = new MMovementLine(Env.getCtx(), 0 , get_TrxName());
		
		try
		{
			moveLine.setM_Movement_ID(move.getM_Movement_ID());
			moveLine.setAD_Org_ID(imove.getAD_Org_ID());
			moveLine.setM_Product_ID(imove.getM_Product_ID());
			moveLine.setM_Locator_ID(imove.getM_Locator_ID());
			moveLine.setM_LocatorTo_ID(imove.getM_LocatorTo_ID());
			moveLine.setMovementQty(imove.getMovementQty());
			moveLine.saveEx();
			imove.setM_MovementLine_ID(moveLine.getM_MovementLine_ID());
			imove.saveEx();			
			isImported = true;
		}
		catch(Exception e)
		{
			imove.setI_ErrorMsg(e.getMessage());
			isImported = false;
		}
		
		return isImported;
	}
	
	/**
	 * Import Inventory Move using X_I_M_Movement table
	 * @param imove X_I_M_Movement
	 * @return MMovement
	 */
	
	private MMovement importMInventoryMove(X_I_Auto_Movement imove)
	{
		final String whereClause = I_M_Movement.COLUMNNAME_MovementDate + "=trunc(cast(? as date)) AND "
	    				  + I_M_Movement.COLUMNNAME_DocumentNo + "=? AND "	  
	    				  + I_M_Movement.COLUMNNAME_C_DocType_ID+"=?";
		int oldID = new Query(Env.getCtx(), I_M_Movement.Table_Name,whereClause, get_TrxName())
		.setClient_ID()
		.setParameters(imove.getMovementDate(), imove.getDocumentNo(), imove.getC_DocType_ID())
		.firstId();
		
		MMovement move = null;
		if(oldID<=0)
		{
			oldID = 0;
		}
		
		move = new MMovement(Env.getCtx(), oldID, get_TrxName());
		
		try{
			move.setDocumentNo(imove.getDocumentNo());
			move.setC_DocType_ID(imove.getC_DocType_ID());
			move.setAD_Org_ID(imove.getAD_Org_ID());
			move.setMovementDate(imove.getMovementDate());
			move.setC_DocType_ID(imove.getC_DocType_ID());
			move.setDocumentNo(imove.getDocumentNo());
			move.setC_BPartner_ID(imove.getC_BPartner_ID());
			move.setM_Shipper_ID(imove.getM_Shipper_ID());
			move.setC_Project_ID(imove.getC_Project_ID());
			move.setC_Campaign_ID(imove.getC_Campaign_ID());
			move.setAD_OrgTrx_ID(imove.getAD_OrgTrx_ID());			
			move.saveEx();
		}
		catch(Exception e)
		{	
			imove.setI_ErrorMsg(e.getMessage());
			isImported = false;
		}
		
		return move;
	}

	/**
	 * The storageAllocation hashmap is used to record the quantities already allocated to a movement line to prevent
	 * double allocating a quantity on hand.
	 */
	private HashMap<MStorageOnHand,BigDecimal> storageAllocation = new HashMap<MStorageOnHand,BigDecimal>();
	private HashMap<X_I_Auto_Movement,BigDecimal> movementShortAllocation = new HashMap<X_I_Auto_Movement,BigDecimal>();

	private BigDecimal getStorageAllocation(MStorageOnHand storage) {
		BigDecimal res = storageAllocation.get(storage);
		if (res == null) {
			res = BigDecimal.ZERO;
		}
		return res;
	}

	private BigDecimal getStorageAvailable(MStorageOnHand storage) {
		return storage.getQtyOnHand().subtract(getStorageAllocation(storage));
	}

	private MStorageOnHand allocateStorageForImportLine(X_I_Auto_Movement imove) {
		MStorageOnHand max_storage = getMaxStorageForImportLine(imove);
		if (max_storage != null) {
			BigDecimal prevAllocation = getStorageAllocation(max_storage);
			BigDecimal qtyAvailable = getStorageAvailable(max_storage);
			BigDecimal qtyToAllocate = imove.getMovementQty();
			if (qtyToAllocate.compareTo(qtyAvailable) > 0) {
				movementShortAllocation.put(imove, qtyToAllocate.subtract(qtyAvailable));
				qtyToAllocate = qtyAvailable;
			}
			storageAllocation.put(max_storage, qtyToAllocate.add(prevAllocation));
		}
		return max_storage;
	}

	private MStorageOnHand getMaxStorageForImportLine(X_I_Auto_Movement imove) {
		Collection<Integer> warehouse_list = new ArrayList<>();
		//when there is no locator with that name we search for a warehouse
		int swarehouse_id = getID(MWarehouse.Table_Name,"Value = ?", new Object[] {imove.getLocatorValue()});
		if (swarehouse_id > 0) {
			warehouse_list.add(swarehouse_id);
		} else {
			// when we could not find a single warehouse we try to find a list of warehouses
			Collection<X_M_Warehouse> wlist = new Query(getCtx(),MWarehouse.Table_Name,"(? like '%|' || Value || '|%')",get_TrxName()).setClient_ID()
			.setParameters(new Object[] {imove.getLocatorValue()}).list();
			for(X_M_Warehouse wh : wlist) {
				warehouse_list.add(wh.getM_Warehouse_ID());
			}
		}
		List<MStorageOnHand> storagelist = new ArrayList<MStorageOnHand>(); 
		for (Integer warehouse_id : warehouse_list) {	
			for(MStorageOnHand s : MStorageOnHand.getWarehouse(getCtx(), warehouse_id, imove.getM_Product_ID(), 0, null, true, true, 0, get_TrxName(), false)) {
				storagelist.add(s);
			}
		}
		// iterate through the entries and remove those without unallocated qtyonhand
		for (Iterator<MStorageOnHand> it=storagelist.iterator(); it.hasNext();) {
		    if (getStorageAvailable(it.next()).compareTo(BigDecimal.ZERO) <= 0)
		        it.remove();
		}
		if (storagelist.size() < 1) {
			return null;
		}
		return Collections.max(storagelist,new Comparator<MStorageOnHand>() {
			@Override
			public int compare(MStorageOnHand o1, MStorageOnHand o2) {
				// sort by 
				// 1. the locator priority
				// 2. the unallocated qtyonhand
				int c1 = Integer.compare(o1.getM_Locator().getPriorityNo(),o2.getM_Locator().getPriorityNo());
				if (c1 != 0) {
					return c1;
				}
				return getStorageAvailable(o1).compareTo(getStorageAvailable(o2));
			}
		});
	}
	
	/**
	 * fill IDs values based on Search Key 
	 */
	private void fillIDValues()
	{
		for(X_I_Auto_Movement imove : getRecords(false, m_IsImportOnlyNoErrors))
		{
			StringBuilder err = new StringBuilder("");
			
			//if(imov.getAD_Org_ID()==0)
				imove.setAD_Org_ID(getID(MOrg.Table_Name,"Value = ?", new Object[]{imove.getOrgValue()}));
			if(imove.getM_Product_ID()==0)
				imove.setM_Product_ID(getID(MProduct.Table_Name,"Value = ?", new Object[]{imove.getProductValue()}));
			//if(imov.getM_Locator_ID()==0)
				imove.setM_Locator_ID(getID(MLocator.Table_Name,"Value = ?", new Object[]{imove.getLocatorValue()}));
				if(imove.getM_Locator_ID() <= 0 && imove.getM_Product_ID() > 0) {
					MStorageOnHand max_storage = allocateStorageForImportLine(imove);
					if (max_storage != null) {
						imove.setM_Locator_ID(max_storage.getM_Locator_ID());
					} else {
						err.append(" @M_Product_ID@/@M_Warehouse_ID@ @DRP-060@,");
					}
				}
			//if(imov.getM_LocatorTo_ID()==0)
				imove.setM_LocatorTo_ID(getID(MLocator.Table_Name,"Value = ?", new Object[]{imove.getLocatorToValue()}));
			if(imove.getC_DocType_ID()==0)
				imove.setC_DocType_ID(getID(MDocType.Table_Name,"Name=?", new Object[]{imove.getDocTypeName()}));
			if(imove.getC_BPartner_ID()==0)
				imove.setC_BPartner_ID(getID(MBPartner.Table_Name,"Value =?", new Object[]{imove.getBPartnerValue()}));
			if(imove.getM_Shipper_ID()==0)
				imove.setM_Shipper_ID(getID(MShipper.Table_Name, "Name = ?", new Object[]{imove.getShipperName()}));
			if(imove.getC_Project_ID()==0)
				imove.setC_Project_ID(getID(MProject.Table_Name, "Value = ?", new Object[]{imove.getProjectValue()}));
			if(imove.getC_Campaign_ID()==0)
				imove.setC_Campaign_ID(getID(MCampaign.Table_Name, "Value = ?", new Object[]{imove.getCampaignValue()}));
			if(imove.getAD_OrgTrx_ID()==0)
				imove.setAD_OrgTrx_ID(getID(MOrg.Table_Name, "Value = ?", new Object[]{imove.getOrgTrxValue()}));
				
			
			imove.saveEx();
			
			if(imove.getAD_Org_ID() <=0)
				err.append(" @AD_Org_ID@ @NotFound@,");
			
			if(imove.getM_Product_ID()<=0)
				err.append(" @M_Product_ID@ @NotFound@,");
			
			if(imove.getM_Locator_ID()<=0)
				err.append(" @M_Locator_ID@ @NotFound@,");
			
			if(imove.getM_LocatorTo_ID()<=0)
				err.append(" @M_LocatorTo_ID@ @NotFound@,");
			
			if(imove.getC_DocType_ID()<=0)
				err.append(" @C_DocType_ID@ @NotFound@,");
			
			MStorageOnHand storage = MStorageOnHand.get(getCtx(), imove.getM_Locator_ID(), imove.getM_Product_ID(), 0, null, get_TrxName());
			if (storage == null || storage.getQtyOnHand().compareTo(BigDecimal.ZERO) <= 0) {
				// there is no qty on hand for this movement line, set an error
				err.append(" @M_Product_ID@/@M_Locator_ID@ @DRP-060@,");
			}
			
			
			if(err.toString()!=null && err.toString().length()>0)
			{
				notimported++;
				imove.setI_ErrorMsg(Msg.parseTranslation(getCtx(), err.toString()));
				imove.saveEx(get_TrxName());
			}		
		}
	}

	/**
	 * Search for Movement Lines to import with insufficient qty on hand at the
	 * assigned source locator and split these lines up if there is another locator
	 * found with qty on hand for the product in question
	 */
	private void splitInsufficientQOHMovements() {
		while (!movementShortAllocation.isEmpty()) {
			X_I_Auto_Movement imove = movementShortAllocation.keySet().iterator().next();
			BigDecimal shortAllocation = movementShortAllocation.remove(imove);
			X_I_Auto_Movement newline = new X_I_Auto_Movement(getCtx(), 0, get_TrxName());
			X_I_Auto_Movement.copyValues(imove, newline);
			newline.setAD_Org_ID(imove.getAD_Org_ID());
			newline.setMovementQty(shortAllocation);
			MStorageOnHand max_storage = allocateStorageForImportLine(newline);
			if (max_storage != null) {
				newline.setM_Locator_ID(max_storage.getM_Locator_ID());
				imove.setMovementQty(imove.getMovementQty().subtract(shortAllocation));
				newline.saveEx(get_TrxName());
				imove.saveEx(get_TrxName());
			} else {
				// here we handle the case when we do not have sufficient qtyonhand for the
				// reqested product. We have two options:
				// 1. If the product is on order we can generate a PO for the product
				// 2. If the product is not on order and the source of this movement is a
				//    customer order, we'll have to cancel the order and report the case as
				//    this should be prevented to happen in the first place
				//
				// TODO: Extend the M_Product Interface to let us find out about the isonorder status
				//       and implement the logic to generate the PO/Order Cancel
				//
				// For now we generate a new line with the missing qty and no locator (as we don't know
				// where to get the product from:
				newline.setI_ErrorMsg(newline.getI_ErrorMsg() + Msg.parseTranslation(getCtx()," @M_Product_ID@/@M_Locator_ID@ @DRP-060@,"));
				imove.setMovementQty(imove.getMovementQty().subtract(shortAllocation));
				newline.saveEx(get_TrxName());
				imove.saveEx(get_TrxName());
			}
		}
	}

	/**
	 * get a record's ID 
	 * @param tableName String
	 * @param whereClause String
	 * @param values Object[]
	 * @return unique record's ID in the table   
	 */
	private int getID(String tableName, String whereClause, Object[] values)
	{
		return new Query(getCtx(),tableName,whereClause,get_TrxName()).setClient_ID()
		.setParameters(values).firstId();
	}  
	
	
	/**
	 * get all records in X_I_Auto_Movement table
	 * @param imported boolean
	 * @param isWithError boolean
	 * @return collection of X_I_Auto_Movement records
	 */
	private Collection<X_I_Auto_Movement> getRecords(boolean imported, boolean isWithError)
	{
		final StringBuffer whereClause = new StringBuffer(X_I_Auto_Movement.COLUMNNAME_I_IsImported)
		.append("=?"); 
		
		if(isWithError)
		{
		    whereClause.append(" AND ").append(X_I_Auto_Movement.COLUMNNAME_I_ErrorMsg).append(" IS NULL");
		}		

		return new Query(getCtx(),X_I_Auto_Movement.Table_Name,whereClause.toString(),get_TrxName())
		.setClient_ID()
		.setParameters(imported)
		.list();
	}	
}	//	Import Auto Inventory Move
