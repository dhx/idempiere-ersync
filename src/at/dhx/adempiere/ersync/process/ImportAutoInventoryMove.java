package at.dhx.adempiere.ersync.process;

import java.sql.Timestamp;

import org.compiere.model.I_M_Movement;
import org.compiere.model.MMovement;
import org.compiere.model.Query;
import org.compiere.model.X_M_Movement;
import org.compiere.util.Env;

import at.dhx.adempiere.ersync.model.X_I_Auto_Movement;

/**
 *	Import Inventory Move from I_Auto_Movement
 *
 *  @author     Daniel Haag (dhx.at)
 */
public class ImportAutoInventoryMove extends ImportExtendedInventoryMove {

	
	private int findMovementId(Timestamp movementDate, String documentNo, int docTypeID) {
		final String whereClause = I_M_Movement.COLUMNNAME_MovementDate + "=trunc(cast(? as date)) AND "
				  + I_M_Movement.COLUMNNAME_DocumentNo + "=? AND "	  
				  + I_M_Movement.COLUMNNAME_C_DocType_ID+"=?";
		return new Query(Env.getCtx(), I_M_Movement.Table_Name,whereClause, get_TrxName())
		.setClient_ID()
		.setParameters(movementDate, documentNo, docTypeID)
		.firstId();
	}
	
	/**
	 * Import Inventory Move using X_I_M_Movement table
	 * In the auto inventory move class we add an autoincrement behavior if there is already an existing
	 * material movement with the same documentno (partial previous import). We add -00n to the documentno
	 * until we find a documentno not taken or a documentno with a material movement not already completed
	 * or closed.
	 * @param imove X_I_M_Movement
	 * @return MMovement
	 */
	@Override
	protected MMovement importMInventoryMove(X_I_Auto_Movement imove)
	{
		int oldID = findMovementId(imove.getMovementDate(), imove.getDocumentNo(), imove.getC_DocType_ID());
		
		MMovement move = null;
		if(oldID<=0)
		{
			oldID = 0;
		}
		
		move = new MMovement(Env.getCtx(), oldID, get_TrxName());
		
		String new_docno = imove.getDocumentNo();
		int docn = 0;
		while (!(move.getDocStatus().equals(X_M_Movement.DOCSTATUS_Drafted) ||
				move.getDocStatus().equals(X_M_Movement.DOCSTATUS_InProgress))) {
			// to the movement we found we cannot add items, so we have to create a new documentno
			docn++;
			new_docno = imove.getDocumentNo() + "-" + String.format("%02d", docn);
			
			oldID = findMovementId(imove.getMovementDate(), new_docno, imove.getC_DocType_ID());
			if(oldID<=0)
			{
				oldID = 0;
			}
			move = new MMovement(Env.getCtx(), oldID, get_TrxName());
		}
		
		try{
			move.setDocumentNo(new_docno);
			move.setC_DocType_ID(imove.getC_DocType_ID());
			move.setAD_Org_ID(imove.getAD_Org_ID());
			move.setMovementDate(imove.getMovementDate());
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
			return null;
		}
		
		return move;
	}
	
	
}
