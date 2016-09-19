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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.logging.Level;

import org.compiere.model.MBPartner;
import org.compiere.model.MBPartnerLocation;
import org.compiere.model.MBankAccount;
import org.compiere.model.MDocType;
import org.compiere.model.MLocation;
import org.compiere.model.MOrder;
import org.compiere.model.MOrderLine;
import org.compiere.model.MPayment;
import org.compiere.model.MTable;
import org.compiere.model.MUser;
import org.compiere.model.PO;
import org.compiere.model.Query;
import org.compiere.model.X_C_Payment;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;

import at.dhx.adempiere.ersync.model.I_I_Extended_Order;
import at.dhx.adempiere.ersync.model.X_I_POS_Order;
import at.dhx.adempiere.ersync.model.X_I_Web_Order;

/**
 * Import Extended Order adapted from Import Order
 * 
 * @author Oscar Gomez <li>BF [ 2936629 ] Error when creating bpartner in the
 *         importation order <li>
 *         https://sourceforge.net/tracker/?func=detail&aid
 *         =2936629&group_id=176962&atid=879332
 * @author Jorg Janke
 * @author modifications for Extended Orders (Web,POS) by Daniel Haag (dhx.at)
 */
public class ImportExtendedOrder extends SvrProcess {
	
	public ImportExtendedOrder() {
		super();
		// TODO Auto-generated constructor stub
	}

	/** Client to be imported to */
	private int m_AD_Client_ID = 0;
	/** Organization to be imported to */
	private int m_AD_Org_ID = 0;
	/** Delete old Imported */
	private boolean m_deleteOldImported = false;
	/** Document Action */
	private String m_docAction = MOrder.DOCACTION_Prepare;
	/** Import only when no errors found */
	private boolean m_isImportOnlyNoErrors = false;

	/** Effective */
	private Timestamp m_DateValue = null;

	private String m_TableName = null;

	public String getM_TableName() {
		return m_TableName;
	}

	public void setM_TableName(String m_TableName) {
		this.m_TableName = m_TableName;
	}

	public I_I_Extended_Order getI_Extended_Order(Properties ctx, ResultSet rs, String trxName) {
		if(getM_TableName().toLowerCase().equals("i_web_order")) {
			return new X_I_Web_Order(getCtx(), rs, get_TrxName());
		} else if(getM_TableName().toLowerCase().equals("i_pos_order")) {
			return new X_I_POS_Order(getCtx(), rs, get_TrxName());
		}
		throw new IllegalStateException("Invalid table for extended order: " + getM_TableName());
	}
	
	/**
	 * Prepare - e.g., get Parameters.
	 */
	protected void prepare() {
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++) {
			String name = para[i].getParameterName();
			if (name.equals("AD_Client_ID")) {
				if (para[i].getParameter() != null)
					m_AD_Client_ID = ((BigDecimal) para[i].getParameter())
							.intValue();
			} else if (name.equals("AD_Org_ID")) {
				if (para[i].getParameter() != null)
					m_AD_Org_ID = ((BigDecimal) para[i].getParameter())
							.intValue();
			} else if (name.equals("DeleteOldImported")) {
				m_deleteOldImported = "Y".equals(para[i].getParameter());
			} else if (name.equals("IsImportOnlyNoErrors")) {
				m_isImportOnlyNoErrors = "Y".equals(para[i].getParameter());
			} else if (name.equals("DocAction")) {
				m_docAction = (String) para[i].getParameter();
			} else {
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
			}
		}
		if (m_DateValue == null)
			m_DateValue = new Timestamp(System.currentTimeMillis());
		if (m_AD_Client_ID == 0)
			m_AD_Client_ID = this.getAD_Client_ID();

		if (m_TableName == null) {
			m_TableName = MTable.getTableName(getCtx(), getProcessInfo().getTable_ID());
		}

	}// prepare

	/**
	 * Perform process.
	 * 
	 * @return Message
	 * @throws Exception
	 */
	protected String doIt() throws java.lang.Exception {
		
		StringBuilder sql = null;
		int no = 0;
		StringBuilder clientCheck = new StringBuilder(" AND AD_Client_ID=")
				.append(m_AD_Client_ID);

		// **** Prepare ****

		// Delete Old Imported
		if (m_deleteOldImported) {
			sql = new StringBuilder("DELETE ").append(getM_TableName())
					.append(" ").append("WHERE I_IsImported='Y'")
					.append(clientCheck);
			no = DB.executeUpdate(sql.toString(), get_TrxName());
			if (log.isLoggable(Level.FINE))
				log.fine("Delete Old Impored =" + no);
		}

		// Set Client, Org, IsActive, Created/Updated
		sql = new StringBuilder("UPDATE ").append(getM_TableName()).append(" ")
				.append("SET AD_Client_ID = COALESCE (AD_Client_ID,")
				.append(m_AD_Client_ID).append("),")
				.append(" AD_Org_ID = COALESCE (AD_Org_ID,")
				.append(m_AD_Org_ID).append("),")
				.append(" IsActive = COALESCE (IsActive, 'Y'),")
				.append(" Created = COALESCE (Created, SysDate),")
				.append(" CreatedBy = COALESCE (CreatedBy, 0),")
				.append(" Updated = COALESCE (Updated, SysDate),")
				.append(" UpdatedBy = COALESCE (UpdatedBy, 0),")
				.append(" I_ErrorMsg = ' ',").append(" I_IsImported = 'N' ")
				.append("WHERE I_IsImported<>'Y' OR I_IsImported IS NULL");
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.INFO))
			log.info("Reset=" + no);

		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Invalid Org, '")
				.append("WHERE (AD_Org_ID IS NULL OR AD_Org_ID=0")
				.append(" OR EXISTS (SELECT * FROM AD_Org oo WHERE o.AD_Org_ID=oo.AD_Org_ID AND (oo.IsSummary='Y' OR oo.IsActive='N')))")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("Invalid Org=" + no);

		// Document Type - PO - SO
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				// PO Document Type Name
				.append("SET C_DocType_ID=(SELECT C_DocType_ID FROM C_DocType d WHERE d.Name=o.DocTypeName")
				.append(" AND d.DocBaseType='POO' AND o.AD_Client_ID=d.AD_Client_ID) ")
				.append("WHERE C_DocType_ID IS NULL AND IsSOTrx='N' AND DocTypeName IS NOT NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set PO DocType=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				// SO Document Type Name
				.append("SET C_DocType_ID=(SELECT C_DocType_ID FROM C_DocType d WHERE d.Name=o.DocTypeName")
				.append(" AND d.DocBaseType='SOO' AND o.AD_Client_ID=d.AD_Client_ID) ")
				.append("WHERE C_DocType_ID IS NULL AND IsSOTrx='Y' AND DocTypeName IS NOT NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set SO DocType=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_DocType_ID=(SELECT C_DocType_ID FROM C_DocType d WHERE d.Name=o.DocTypeName")
				.append(" AND d.DocBaseType IN ('SOO','POO') AND o.AD_Client_ID=d.AD_Client_ID) ")
				// +
				// "WHERE C_DocType_ID IS NULL AND IsSOTrx IS NULL AND DocTypeName IS NOT NULL AND I_IsImported<>'Y'").append
				// (clientCheck);
				.append("WHERE C_DocType_ID IS NULL AND DocTypeName IS NOT NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set DocType=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				// Error Invalid Doc Type Name
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Invalid DocTypeName, ' ")
				.append("WHERE C_DocType_ID IS NULL AND DocTypeName IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("Invalid DocTypeName=" + no);
		// DocType Default
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				// Default PO
				.append("SET C_DocType_ID=(SELECT MAX(C_DocType_ID) FROM C_DocType d WHERE d.IsDefault='Y'")
				.append(" AND d.DocBaseType='POO' AND o.AD_Client_ID=d.AD_Client_ID) ")
				.append("WHERE C_DocType_ID IS NULL AND IsSOTrx='N' AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set PO Default DocType=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				// Default SO
				.append("SET C_DocType_ID=(SELECT MAX(C_DocType_ID) FROM C_DocType d WHERE d.IsDefault='Y'")
				.append(" AND d.DocBaseType='SOO' AND o.AD_Client_ID=d.AD_Client_ID) ")
				.append("WHERE C_DocType_ID IS NULL AND IsSOTrx='Y' AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set SO Default DocType=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_DocType_ID=(SELECT MAX(C_DocType_ID) FROM C_DocType d WHERE d.IsDefault='Y'")
				.append(" AND d.DocBaseType IN('SOO','POO') AND o.AD_Client_ID=d.AD_Client_ID) ")
				.append("WHERE C_DocType_ID IS NULL AND IsSOTrx IS NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Default DocType=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				// No DocType
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=No DocType, ' ")
				.append("WHERE C_DocType_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No DocType=" + no);

		// Set IsSOTrx
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o SET IsSOTrx='Y' ")
				.append("WHERE EXISTS (SELECT * FROM C_DocType d WHERE o.C_DocType_ID=d.C_DocType_ID AND d.DocBaseType='SOO' AND o.AD_Client_ID=d.AD_Client_ID)")
				.append(" AND C_DocType_ID IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set IsSOTrx=Y=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o SET IsSOTrx='N' ")
				.append("WHERE EXISTS (SELECT * FROM C_DocType d WHERE o.C_DocType_ID=d.C_DocType_ID AND d.DocBaseType='POO' AND o.AD_Client_ID=d.AD_Client_ID)")
				.append(" AND C_DocType_ID IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set IsSOTrx=N=" + no);

		// Currency ISO Code
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_Currency_ID=(SELECT C_Currency_ID FROM C_Currency c")
				.append(" WHERE (o.AD_Client_ID=c.AD_Client_ID OR c.AD_Client_ID = 0)")
				.append(" AND c.ISO_Code = o.ISO_Code) ")
				.append("WHERE C_Currency_ID IS NULL AND ISO_Code IS NOT NULL AND ISO_Code <> ''")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			if (log.isLoggable(Level.FINE))
				log.fine("Set Currency from ISO Code=" + no);
		//
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=No Currency, ' ")
				.append("WHERE C_Currency_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No Currency=" + no);

		// Price List
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_PriceList_ID=(SELECT MAX(M_PriceList_ID) FROM M_PriceList p WHERE p.IsDefault='Y'")
				.append(" AND p.C_Currency_ID=o.C_Currency_ID AND p.IsSOPriceList=o.IsSOTrx AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE M_PriceList_ID IS NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Default Currency PriceList=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_PriceList_ID=(SELECT MAX(M_PriceList_ID) FROM M_PriceList p WHERE p.IsDefault='Y'")
				.append(" AND p.IsSOPriceList=o.IsSOTrx AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE M_PriceList_ID IS NULL AND C_Currency_ID IS NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Default PriceList=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_PriceList_ID=(SELECT MAX(M_PriceList_ID) FROM M_PriceList p ")
				.append(" WHERE p.C_Currency_ID=o.C_Currency_ID AND p.IsSOPriceList=o.IsSOTrx AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE M_PriceList_ID IS NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Currency PriceList=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_PriceList_ID=(SELECT MAX(M_PriceList_ID) FROM M_PriceList p ")
				.append(" WHERE p.IsSOPriceList=o.IsSOTrx AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE M_PriceList_ID IS NULL AND C_Currency_ID IS NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set PriceList=" + no);
		//
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=No PriceList, ' ")
				.append("WHERE M_PriceList_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No PriceList=" + no);

		// @Trifon - Import Order Source
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_OrderSource_ID=(SELECT C_OrderSource_ID FROM C_OrderSource p")
				.append(" WHERE o.C_OrderSourceValue=p.Value AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE C_OrderSource_ID IS NULL AND C_OrderSourceValue IS NOT NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Order Source=" + no);
		// Set proper error message
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Not Found Order Source, ' ")
				.append("WHERE C_OrderSource_ID IS NULL AND C_OrderSourceValue IS NOT NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No OrderSource=" + no);

		// Payment Term
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_PaymentTerm_ID=(SELECT C_PaymentTerm_ID FROM C_PaymentTerm p")
				.append(" WHERE o.PaymentTermValue=p.Value AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE C_PaymentTerm_ID IS NULL AND PaymentTermValue IS NOT NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set PaymentTerm=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_PaymentTerm_ID=(SELECT MAX(C_PaymentTerm_ID) FROM C_PaymentTerm p")
				.append(" WHERE p.IsDefault='Y' AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE C_PaymentTerm_ID IS NULL AND o.PaymentTermValue IS NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Default PaymentTerm=" + no);
		//
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=No PaymentTerm, ' ")
				.append("WHERE C_PaymentTerm_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No PaymentTerm=" + no);

		// Warehouse
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_Warehouse_ID=(SELECT MAX(M_Warehouse_ID) FROM M_Warehouse w")
				.append(" WHERE o.AD_Client_ID=w.AD_Client_ID AND o.AD_Org_ID=w.AD_Org_ID) ")
				.append("WHERE M_Warehouse_ID IS NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName()); // Warehouse for
																// Org
		if (no != 0)
			if (log.isLoggable(Level.FINE))
				log.fine("Set Warehouse=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_Warehouse_ID=(SELECT M_Warehouse_ID FROM M_Warehouse w")
				.append(" WHERE o.AD_Client_ID=w.AD_Client_ID) ")
				.append("WHERE M_Warehouse_ID IS NULL")
				.append(" AND EXISTS (SELECT AD_Client_ID FROM M_Warehouse w WHERE w.AD_Client_ID=o.AD_Client_ID GROUP BY AD_Client_ID HAVING COUNT(*)=1)")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			if (log.isLoggable(Level.FINE))
				log.fine("Set Only Client Warehouse=" + no);
		//
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=No Warehouse, ' ")
				.append("WHERE M_Warehouse_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No Warehouse=" + no);

		// BP from EMail when no BPartnerValue is set
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET (C_BPartner_ID,AD_User_ID)=(SELECT C_BPartner_ID,AD_User_ID FROM AD_User u")
				.append(" WHERE o.EMail=u.EMail AND o.AD_Client_ID=u.AD_Client_ID AND u.C_BPartner_ID IS NOT NULL) ")
				.append("WHERE C_BPartner_ID IS NULL AND BPartnerValue IS NULL AND EMail IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set BP from EMail=" + no);
		// BP from ContactName when no BPartnerValue is set
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET (C_BPartner_ID,AD_User_ID)=(SELECT C_BPartner_ID,AD_User_ID FROM AD_User u")
				.append(" WHERE o.ContactName=u.Name AND o.AD_Client_ID=u.AD_Client_ID AND u.C_BPartner_ID IS NOT NULL) ")
				.append("WHERE C_BPartner_ID IS NULL AND BPartnerValue IS NULL AND ContactName IS NOT NULL")
				.append(" AND EXISTS (SELECT Name FROM AD_User u WHERE o.ContactName=u.Name AND o.AD_Client_ID=u.AD_Client_ID AND u.C_BPartner_ID IS NOT NULL GROUP BY Name HAVING COUNT(*)=1)")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set BP from ContactName=" + no);
		// BP from Value
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_BPartner_ID=(SELECT MAX(C_BPartner_ID) FROM C_BPartner bp")
				.append(" WHERE o.BPartnerValue=bp.Value AND o.AD_Client_ID=bp.AD_Client_ID) ")
				.append("WHERE C_BPartner_ID IS NULL AND BPartnerValue IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set BP from Value=" + no);
		// Default BP
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET (C_BPartner_ID,BPartnerValue)=(SELECT c.C_BPartnerCashTrx_ID,bp.Value FROM AD_ClientInfo c")
				.append(" INNER JOIN C_BPartner bp ON bp.C_BPartner_ID = c.C_BPartnerCashTrx_ID")
				.append(" WHERE o.AD_Client_ID=c.AD_Client_ID) ")
				.append("WHERE C_BPartner_ID IS NULL AND BPartnerValue IS NULL AND Name IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Default BP=" + no);

		// Set Country
		/**
		 * sql = new StringBuffer
		 * ("UPDATE ").append(getM_TableName()).append(" o " +
		 * "SET CountryCode=(SELECT MAX(CountryCode) FROM C_Country c WHERE c.IsDefault='Y'"
		 * + " AND c.AD_Client_ID IN (0, o.AD_Client_ID)) " +
		 * "WHERE C_BPartner_ID IS NULL AND CountryCode IS NULL AND C_Country_ID IS NULL"
		 * + " AND I_IsImported<>'Y'").append (clientCheck); no =
		 * DB.executeUpdate(sql.toString(), get_TrxName());
		 * log.fine("Set Country Default=" + no);
		 **/
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_Country_ID=(SELECT C_Country_ID FROM C_Country c")
				.append(" WHERE o.CountryCode=c.CountryCode AND c.AD_Client_ID IN (0, o.AD_Client_ID)) ")
				.append("WHERE C_Country_ID IS NULL")
				.append(" AND CountryCode IS NOT NULL AND CountryCode<>''")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Country=" + no);
		//
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Invalid Country, ' ")
				.append("WHERE C_BPartner_ID IS NULL AND C_Country_ID IS NULL")
				.append(" AND CountryCode IS NOT NULL AND CountryCode<>''")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("Invalid Country=" + no);


		// Existing Location ? Exact Match BillTo
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET BillTo_ID=(SELECT C_BPartner_Location_ID")
				.append(" FROM C_BPartner_Location bpl INNER JOIN C_Location l ON (bpl.C_Location_ID=l.C_Location_ID)")
				.append(" WHERE o.C_BPartner_ID=bpl.C_BPartner_ID AND bpl.AD_Client_ID=o.AD_Client_ID")
				.append(" AND DUMP(o.Address1)=DUMP(l.Address1) AND DUMP(o.Address2)=DUMP(l.Address2)")
				.append(" AND DUMP(o.City)=DUMP(l.City) AND DUMP(o.Postal)=DUMP(l.Postal)")
				.append(" AND o.C_Region_ID=l.C_Region_ID AND o.C_Country_ID=l.C_Country_ID) ")
				.append("WHERE C_BPartner_ID IS NOT NULL AND BillTo_ID IS NULL")
				.append(" AND (IsBillTo='Y' OR IsBillTo IS NULL)")
				.append(" AND I_IsImported='N'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Found Location=" + no);

		// Existing Location ? Exact Match Delivery
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_BPartner_Location_ID=(SELECT C_BPartner_Location_ID")
				.append(" FROM C_BPartner_Location bpl INNER JOIN C_Location l ON (bpl.C_Location_ID=l.C_Location_ID)")
				.append(" WHERE o.C_BPartner_ID=bpl.C_BPartner_ID AND bpl.AD_Client_ID=o.AD_Client_ID")
				.append(" AND DUMP(o.Address1)=DUMP(l.Address1) AND DUMP(o.Address2)=DUMP(l.Address2)")
				.append(" AND DUMP(o.City)=DUMP(l.City) AND DUMP(o.Postal)=DUMP(l.Postal)")
				.append(" AND o.C_Region_ID=l.C_Region_ID AND o.C_Country_ID=l.C_Country_ID) ")
				.append("WHERE C_BPartner_ID IS NOT NULL AND C_BPartner_Location_ID IS NULL")
				.append(" AND (IsBillTo='N' OR IsBillTo IS NULL)")
				.append(" AND I_IsImported='N'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Found Location=" + no);

		/*
		 * DHX: We do not set the Bill and Delivery Location from the BPartner
		 * as the Locations will always be set from the Web Order
		 *//**
		 * // Set Bill Location from BPartner sql = new StringBuilder
		 * ("UPDATE ").append(getM_TableName()).append(" o ") .append(
		 * "SET BillTo_ID=(SELECT MAX(C_BPartner_Location_ID) FROM C_BPartner_Location l"
		 * ) .append(
		 * " WHERE l.C_BPartner_ID=o.C_BPartner_ID AND o.AD_Client_ID=l.AD_Client_ID"
		 * ) .append(
		 * " AND ((l.IsBillTo='Y' AND o.IsSOTrx='Y') OR (l.IsPayFrom='Y' AND o.IsSOTrx='N'))"
		 * ) .append(") ")
		 * .append("WHERE C_BPartner_ID IS NOT NULL AND BillTo_ID IS NULL")
		 * .append(" AND I_IsImported<>'Y'").append (clientCheck); no =
		 * DB.executeUpdate(sql.toString(), get_TrxName()); if
		 * (log.isLoggable(Level.FINE)) log.fine("Set BP BillTo from BP=" + no);
		 * 
		 * // Set Location from BPartner sql = new StringBuilder
		 * ("UPDATE ").append(getM_TableName()).append(" o ") .append(
		 * "SET C_BPartner_Location_ID=(SELECT MAX(C_BPartner_Location_ID) FROM C_BPartner_Location l"
		 * ) .append(
		 * " WHERE l.C_BPartner_ID=o.C_BPartner_ID AND o.AD_Client_ID=l.AD_Client_ID"
		 * ) .append(
		 * " AND ((l.IsShipTo='Y' AND o.IsSOTrx='Y') OR o.IsSOTrx='N')")
		 * .append(") ") .append(
		 * "WHERE C_BPartner_ID IS NOT NULL AND C_BPartner_Location_ID IS NULL")
		 * .append(" AND I_IsImported<>'Y'").append (clientCheck); no =
		 * DB.executeUpdate(sql.toString(), get_TrxName()); if
		 * (log.isLoggable(Level.FINE)) log.fine("Set BP Location from BP=" +
		 * no);
		 **/

		
		
		// Set Region
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("Set RegionName=(SELECT MAX(Name) FROM C_Region r")
				.append(" WHERE r.IsDefault='Y' AND r.C_Country_ID=o.C_Country_ID")
				.append(" AND r.AD_Client_ID IN (0, o.AD_Client_ID)) ")
				.append("WHERE C_BPartner_ID IS NULL AND C_Region_ID IS NULL AND RegionName IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Region Default=" + no);
		//
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("Set C_Region_ID=(SELECT C_Region_ID FROM C_Region r")
				.append(" WHERE r.Name=o.RegionName AND r.C_Country_ID=o.C_Country_ID")
				.append(" AND r.AD_Client_ID IN (0, o.AD_Client_ID)) ")
				.append("WHERE C_BPartner_ID IS NULL AND C_Region_ID IS NULL AND RegionName IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Region=" + no);
		//
		/**
		 * sql = new StringBuilder
		 * ("UPDATE ").append(getM_TableName()).append(" o ") .append(
		 * "SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Invalid Region, ' "
		 * ) .append("WHERE C_BPartner_ID IS NULL AND C_Region_ID IS NULL ")
		 * .append(" AND EXISTS (SELECT * FROM C_Country c")
		 * .append(" WHERE c.C_Country_ID=o.C_Country_ID AND c.HasRegion='Y')")
		 * .append(" AND I_IsImported<>'Y'").append (clientCheck); no =
		 * DB.executeUpdate(sql.toString(), get_TrxName()); if (no != 0)
		 * log.warning ("Invalid Region=" + no);
		 **/

		// Product
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_Product_ID=(SELECT MAX(M_Product_ID) FROM M_Product p")
				.append(" WHERE o.ProductValue=p.Value AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE M_Product_ID IS NULL AND ProductValue IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Product from Value=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_Product_ID=(SELECT MAX(M_Product_ID) FROM M_Product p")
				.append(" WHERE trim(leading ' 0' from o.UPC)=p.UPC AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE M_Product_ID IS NULL AND UPC IS NOT NULL AND UPC <> ''")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Product from UPC=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_Product_ID=(SELECT MAX(M_Product_ID) FROM M_Product p")
				.append(" WHERE o.SKU=p.SKU AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE M_Product_ID IS NULL AND SKU IS NOT NULL AND SKU <> ''")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Product fom SKU=" + no);
		// As a last resort try to associate the UPC by the M_Product_PO UPC if one is available
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET M_Product_ID=(SELECT MAX(M_Product_ID) FROM M_Product_PO p")
				.append(" WHERE trim(leading ' 0' from o.UPC)=p.UPC AND o.AD_Client_ID=p.AD_Client_ID) ")
				.append("WHERE M_Product_ID IS NULL AND UPC IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append (clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE)) log.fine("Set Product from PO UPC=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Invalid Product, ' ")
				.append("WHERE M_Product_ID IS NULL AND (")
				.append(" ProductValue IS NOT NULL")
				.append(" OR (UPC IS NOT NULL AND UPC <> '') ")
				.append(" OR (SKU IS NOT NULL AND SKU <> '') ")
				.append(") AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("Invalid Product=" + no);

		// See if product is in pricelist
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Product not in valid price list, ' ")
				.append("WHERE o.M_Product_ID IS NOT NULL AND o.M_PriceList_ID IS NOT NULL")
				.append(" AND o.M_PriceList_ID NOT IN")
				.append(" (SELECT v.M_PriceList_ID FROM M_PriceList_Version v, M_ProductPrice p")
				.append(" WHERE v.m_pricelist_version_id = p.m_pricelist_version_id ")
				.append(" AND p.m_product_id = o.M_Product_ID AND v.validfrom <= o.dateordered)")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("Invalid Product=" + no);

		// Charge
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_Charge_ID=(SELECT C_Charge_ID FROM C_Charge c")
				.append(" WHERE o.ChargeName=c.Name AND o.AD_Client_ID=c.AD_Client_ID) ")
				.append("WHERE C_Charge_ID IS NULL AND ChargeName IS NOT NULL AND I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Charge=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Invalid Charge, ' ")
				.append("WHERE C_Charge_ID IS NULL AND (ChargeName IS NOT NULL)")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("Invalid Charge=" + no);
		//

		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Product and Charge, ' ")
				.append("WHERE M_Product_ID IS NOT NULL AND C_Charge_ID IS NOT NULL ")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("Invalid Product and Charge exclusive=" + no);

		// Tax
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_Tax_ID=(SELECT MAX(C_Tax_ID) FROM C_Tax t")
				.append(" WHERE o.TaxIndicator=t.TaxIndicator AND o.AD_Client_ID=t.AD_Client_ID")
				.append(" AND (t.sopotype = 'B' or t.sopotype = IF(o.IsSOTrx='Y','S','P'))) ")
				.append("WHERE C_Tax_ID IS NULL AND TaxIndicator IS NOT NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set Tax=" + no);
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=Invalid Tax, ' ")
				.append("WHERE C_Tax_ID IS NULL AND TaxIndicator IS NOT NULL AND (")
				.append(" ProductValue IS NOT NULL")
				.append(" OR M_Product_ID IS NOT NULL")
				.append(" OR (UPC IS NOT NULL AND UPC <> '') ")
				.append(" OR (SKU IS NOT NULL AND SKU <> '') ")
				.append(") AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("Invalid Tax=" + no);

		commitEx();

		// -- New BPartner ---------------------------------------------------

		// Go through Order Records w/o
		// C_BPartner_ID/C_BPartner_Location_ID/BillTo_ID
		sql = new StringBuilder("SELECT * FROM ").append(getM_TableName())
				.append(" ").append("WHERE I_IsImported<>'Y'")
				.append(" AND (C_BPartner_ID IS NULL")
				.append(" OR C_BPartner_Location_ID IS NULL")
				.append(" OR BillTo_ID IS NULL)").append(clientCheck)
				.append(" ORDER BY ").append(getM_TableName())
				.append("_ID ASC");
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = DB.prepareStatement(sql.toString(), get_TrxName());
			rs = pstmt.executeQuery();
			while (rs.next()) {
				I_I_Extended_Order imp = this.getI_Extended_Order(getCtx(), rs,	get_TrxName());
				if (imp.getBPartnerValue() == null) {
					if (imp.getEMail() != null)
						imp.setBPartnerValue(imp.getEMail().toLowerCase());
					// else if (imp.getName () != null)
					// imp.setBPartnerValue (imp.getName ());
					else
						continue;
				}
				if (imp.getName() == null || imp.getName().equals("")) {
					if (imp.getContactName() != null)
						imp.setName(imp.getContactName());
					else
						imp.setName(imp.getBPartnerValue());
				}
				// BPartner
				MBPartner bp;
				if (imp.getC_BPartner_ID() > 0) {
					bp = MBPartner.get(getCtx(), imp.getC_BPartner_ID(),
							get_TrxName());
				} else {
					bp = MBPartner.get(getCtx(), imp.getBPartnerValue(),
							get_TrxName());
				}
				if (bp == null) {
					if (imp.getName() == null || imp.getName().length() <= 0)
						continue;

					bp = new MBPartner(getCtx(), -1, get_TrxName());
					bp.setClientOrg(imp.getAD_Client_ID(), 0);
					bp.setValue(imp.getBPartnerValue());
					bp.setName(imp.getName());
					if (!bp.save())
						continue;
				}
				imp.setC_BPartner_ID(bp.getC_BPartner_ID());

				// BP Location
				int i_location_id = imp.isBillTo() ? imp.getBillTo_ID() : imp
						.getC_BPartner_Location_ID();
				MBPartnerLocation bpl = null;
				MBPartnerLocation[] bpls = bp.getLocations(true);
				for (int i = 0; bpl == null && i < bpls.length; i++) {
					if (i_location_id == bpls[i].getC_BPartner_Location_ID())
						bpl = bpls[i];
					// Same Location Info
					else if (i_location_id == 0) {
						MLocation loc = bpls[i].getLocation(false);
						if (loc.equals(imp.getC_Country_ID(),
								imp.getC_Region_ID(), imp.getPostal(), "",
								imp.getCity(), imp.getAddress1(),
								imp.getAddress2()))
							bpl = bpls[i];
					}
				}
				if (bpl == null && imp.getC_Country_ID() > 0) {
					// New Location
					MLocation loc = new MLocation(getCtx(), 0, get_TrxName());
					loc.setAddress1(imp.getAddress1());
					loc.setAddress2(imp.getAddress2());
					loc.setCity(imp.getCity());
					loc.setPostal(imp.getPostal());
					if (imp.getC_Region_ID() != 0)
						loc.setC_Region_ID(imp.getC_Region_ID());
					loc.setC_Country_ID(imp.getC_Country_ID());
					if (!loc.save())
						continue;
					//
					bpl = new MBPartnerLocation(bp);
					bpl.setC_Location_ID(loc.getC_Location_ID());
					if (!bpl.save())
						continue;
				}
				if (bpl != null) {
					if (imp.isBillTo()) {
						imp.setBillTo_ID(bpl.getC_BPartner_Location_ID());
					} else {
						imp.setC_BPartner_Location_ID(bpl
								.getC_BPartner_Location_ID());
					}
				}

				// User/Contact
				if ((imp.getContactName() != null && imp.getContactName()
						.length() > 0)
						|| (imp.getEMail() != null && imp.getEMail().length() > 0)
						|| (imp.getPhone() != null && imp.getPhone().length() > 0)) {
					MUser[] users = bp.getContacts(true);
					MUser user = null;
					for (int i = 0; user == null && i < users.length; i++) {
						String name = users[i].getName();
						if (name.equals(imp.getContactName())
								|| name.equals(imp.getName())) {
							user = users[i];
						}
					}
					if (user == null) {
						user = new MUser(bp);
						if (imp.getContactName() == null)
							user.setName(imp.getName());
						else
							user.setName(imp.getContactName());
						user.setEMail(imp.getEMail());
						user.setPhone(imp.getPhone());
						if (!user.save()) {
							user = null;
						}
					}
					if (user != null) {
						if (imp.isBillTo()) {
							imp.setBill_User_ID(user.getAD_User_ID());
						} else {
							imp.setAD_User_ID(user.getAD_User_ID());
						}
					}
				}
				((PO) imp).save();
			} // for all Order Records w/o
				// C_BPartner_ID/C_BPartner_Location_ID/BillTo_ID
			//
		} catch (SQLException e) {
			log.log(Level.SEVERE, "BP - " + sql.toString(), e);
		} finally {
			DB.close(rs, pstmt);
			rs = null;
			pstmt = null;
		}

		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET C_BPartner_Location_ID=(SELECT MAX(C_BPartner_Location_ID) FROM ")
				.append(getM_TableName())
				.append(" wo")
				.append(" WHERE wo.DocumentNo = o.DocumentNo AND wo.C_BPartner_ID=o.C_BPartner_ID")
				.append(" AND wo.AD_Client_ID=o.AD_Client_ID AND wo.AD_Org_ID=o.AD_Org_ID")
				.append(") ")
				.append("WHERE C_BPartner_ID IS NOT NULL AND C_BPartner_Location_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set BP Location from other line=" + no);

		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET BillTo_ID=(SELECT MAX(BillTo_ID) FROM ")
				.append(getM_TableName())
				.append(" wo")
				.append(" WHERE wo.DocumentNo = o.DocumentNo AND wo.C_BPartner_ID=o.C_BPartner_ID")
				.append(" AND wo.AD_Client_ID=o.AD_Client_ID AND wo.AD_Org_ID=o.AD_Org_ID")
				.append(") ")
				.append("WHERE C_BPartner_ID IS NOT NULL AND BillTo_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set BillTo Location from other line=" + no);

		// If there is no bpartner location set until now we take the bill to address to ship to
		sql = new StringBuilder("UPDATE ")
			.append(getM_TableName())
			.append(" o ")
			.append("SET C_BPartner_Location_ID=(SELECT MAX(BillTo_ID) FROM ")
			.append(getM_TableName())
			.append(" wo")
			.append(" WHERE wo.DocumentNo = o.DocumentNo AND wo.C_BPartner_ID=o.C_BPartner_ID")
			.append(" AND wo.AD_Client_ID=o.AD_Client_ID AND wo.AD_Org_ID=o.AD_Org_ID")
			.append(") ")
			.append("WHERE C_BPartner_ID IS NOT NULL AND C_BPartner_Location_ID IS NULL")
			.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set BP Location from BillTo Address=" + no);		
		
		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET PaidAmt=(SELECT MAX(PaidAmt) FROM ")
				.append(getM_TableName())
				.append(" wo")
				.append(" WHERE wo.DocumentNo = o.DocumentNo AND wo.C_BPartner_ID=o.C_BPartner_ID")
				.append(" AND wo.AD_Client_ID=o.AD_Client_ID AND wo.AD_Org_ID=o.AD_Org_ID")
				.append(") ").append("WHERE I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set PaidAmt from other line=" + no);

		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" o ")
				.append("SET FreightAmt=(SELECT MAX(FreightAmt) FROM ")
				.append(getM_TableName())
				.append(" wo")
				.append(" WHERE wo.DocumentNo = o.DocumentNo AND wo.C_BPartner_ID=o.C_BPartner_ID")
				.append(" AND wo.AD_Client_ID=o.AD_Client_ID AND wo.AD_Org_ID=o.AD_Org_ID")
				.append(") ").append("WHERE I_IsImported<>'Y'")
				.append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (log.isLoggable(Level.FINE))
			log.fine("Set FreightAmt from other line=" + no);

		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=No BP Location, ' ")
				.append("WHERE C_BPartner_ID IS NOT NULL AND C_BPartner_Location_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No BP Location=" + no);

		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=No BillTo Location, ' ")
				.append("WHERE C_BPartner_ID IS NOT NULL AND BillTo_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No BillTo Location=" + no);

		sql = new StringBuilder("UPDATE ")
				.append(getM_TableName())
				.append(" ")
				.append("SET I_IsImported='E', I_ErrorMsg=I_ErrorMsg||'ERR=No BPartner, ' ")
				.append("WHERE C_BPartner_ID IS NULL")
				.append(" AND I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		if (no != 0)
			log.warning("No BPartner=" + no);

		commitEx();

		// -- New Orders -----------------------------------------------------

		// check how many lines there are with error status
		sql = new StringBuilder("SELECT COUNT(*) c FROM ")
				.append(getM_TableName())
				.append(" WHERE I_IsImported='E'")
				.append(clientCheck);
		no = DB.getSQLValue(get_TrxName(), sql.toString());
		if (no != 0)
			log.warning("No Import lines with Error=" + no);

		int noInsert = 0;
		int noInsertLine = 0;

		if (no != 0 && m_isImportOnlyNoErrors) {
			log.warning("No Lines imported as isImportOnlyNoErrors parameter is set to true");
		} else {
			// Go through Order Records w/o
			sql = new StringBuilder("SELECT * FROM ")
					.append(getM_TableName())
					.append(" WHERE I_IsImported='N'")
					.append(" AND documentno NOT IN (SELECT DISTINCT documentno FROM ")
					.append(getM_TableName())
					.append(" WHERE I_IsImported='E')")
					.append(clientCheck)
					.append(" ORDER BY C_BPartner_ID, BillTo_ID, C_BPartner_Location_ID, ")
					.append(getM_TableName()).append("_ID");
			try {
				pstmt = DB.prepareStatement(sql.toString(), get_TrxName());
				rs = pstmt.executeQuery();
				//
				int oldC_BPartner_ID = 0;
				int oldBillTo_ID = 0;
				int oldC_BPartner_Location_ID = 0;
				String oldDocumentNo = "";
				//
				MOrder order = null;
				int lineNo = 0;
				while (rs.next()) {
					I_I_Extended_Order imp = this.getI_Extended_Order(getCtx(), rs, get_TrxName());
						
					String cmpDocumentNo = imp.getDocumentNo();
					if (cmpDocumentNo == null)
						cmpDocumentNo = "";
					// New Order
					if (oldC_BPartner_ID != imp.getC_BPartner_ID()
							|| oldC_BPartner_Location_ID != imp
									.getC_BPartner_Location_ID()
							|| oldBillTo_ID != imp.getBillTo_ID()
							|| !oldDocumentNo.equals(cmpDocumentNo)) {
						if (order != null) {
							if (m_docAction != null && m_docAction.length() > 0) {
								order.setDocAction(m_docAction);
								if (!order.processIt(m_docAction)) {
									log.warning("Order Process Failed: "
											+ order + " - "
											+ order.getProcessMsg());
									// throw new
									// IllegalStateException("Order Process Failed: "
									// + order + " - " + order.getProcessMsg());

								}
							}
							order.saveEx();
						}
						oldC_BPartner_ID = imp.getC_BPartner_ID();
						oldC_BPartner_Location_ID = imp
								.getC_BPartner_Location_ID();
						oldBillTo_ID = imp.getBillTo_ID();
						oldDocumentNo = imp.getDocumentNo();
						if (oldDocumentNo == null)
							oldDocumentNo = "";
						//
						order = new MOrder(getCtx(), 0, get_TrxName());
						order.setClientOrg(imp.getAD_Client_ID(),
								imp.getAD_Org_ID());
						order.setC_DocTypeTarget_ID(imp.getC_DocType_ID());
						order.setIsSOTrx(imp.isSOTrx());
						if (imp.getDeliveryRule() != null) {
							order.setDeliveryRule(imp.getDeliveryRule());
						}
						if (imp.getDocumentNo() != null)
							order.setDocumentNo(imp.getDocumentNo());
						// Ship Partner
						order.setC_BPartner_ID(imp.getC_BPartner_ID());
						order.setC_BPartner_Location_ID(imp
								.getC_BPartner_Location_ID());
						if (imp.getAD_User_ID() != 0)
							order.setAD_User_ID(imp.getAD_User_ID());
						// Bill Partner
						order.setBill_BPartner_ID(imp.getC_BPartner_ID());
						order.setBill_Location_ID(imp.getBillTo_ID());
						//
						if (imp.getDescription() != null)
							order.setDescription(imp.getDescription());
						order.setC_PaymentTerm_ID(imp.getC_PaymentTerm_ID());
						order.setM_PriceList_ID(imp.getM_PriceList_ID());
						order.setM_Warehouse_ID(imp.getM_Warehouse_ID());
						if (imp.getM_Shipper_ID() != 0)
							order.setM_Shipper_ID(imp.getM_Shipper_ID());
						// SalesRep from Import or the person running the import
						if (imp.getSalesRep_ID() != 0)
							order.setSalesRep_ID(imp.getSalesRep_ID());
						if (order.getSalesRep_ID() == 0)
							order.setSalesRep_ID(getAD_User_ID());
						//
						if (imp.getAD_OrgTrx_ID() != 0)
							order.setAD_OrgTrx_ID(imp.getAD_OrgTrx_ID());
						if (imp.getC_Activity_ID() != 0)
							order.setC_Activity_ID(imp.getC_Activity_ID());
						if (imp.getC_Campaign_ID() != 0)
							order.setC_Campaign_ID(imp.getC_Campaign_ID());
						if (imp.getC_Project_ID() != 0)
							order.setC_Project_ID(imp.getC_Project_ID());
						//
						if (imp.getDateOrdered() != null)
							order.setDateOrdered(imp.getDateOrdered());
						if (imp.getDateAcct() != null)
							order.setDateAcct(imp.getDateAcct());

						// Set Order Source
						if (imp.getC_OrderSource() != null)
							order.setC_OrderSource_ID(imp.getC_OrderSource_ID());
						//

						if (imp.getFreightAmt() != null
								&& imp.getFreightAmt().signum() != 0) {
							order.setFreightAmt(imp.getFreightAmt());							
							// Set the freight cost rule to fixed
							order.setFreightCostRule("F");
							order.setDeliveryViaRule(MOrder.DELIVERYVIARULE_Shipper);
							order.setM_Shipper_ID(1000002);
						}

						order.saveEx();

						if (imp.getPaidAmt() != null
								&& imp.getPaidAmt().signum() != 0) {
							// create payment for the order
							MPayment payment = new MPayment(getCtx(), 0,
									get_TrxName());
							payment.setAD_Org_ID(order.getAD_Org_ID());
							payment.setDocumentNo(order.getDocumentNo());
							payment.setC_Order_ID(order.getC_Order_ID());

							payment.setTrxType(X_C_Payment.TRXTYPE_Sales);
							payment.setTenderType(X_C_Payment.TENDERTYPE_Account);

							if (imp.getC_BankAccount_ID() > 0) {
								payment.setC_BankAccount_ID(imp
										.getC_BankAccount_ID());
							} else {
								// if no BankAccount is defined for the import
								// we try to find
								// the default for the target org
								int account_id = new Query(getCtx(),
										MBankAccount.Table_Name,
										"AD_Org_ID = ? AND isdefault = 'Y'",
										get_TrxName())
										.setClient_ID()
										.setOnlyActiveRecords(true)
										.setParameters(
												new Object[] { order
														.getAD_Org_ID() })
										.firstId();

								payment.setC_BankAccount_ID(account_id);
							}

							payment.setDateAcct(order.getDateAcct());
							payment.setDateTrx(order.getDateOrdered());
							// payment.setDescription(imp.getDescription());
							payment.setC_BPartner_ID(order.getC_BPartner_ID());

							String doc_basetype = order.isSOTrx() ? "ARR"
									: "APP";
							int payment_doctype_id = new Query(getCtx(),
									MDocType.Table_Name, "DocBaseType = ?",
									get_TrxName())
									.setClient_ID()
									.setOnlyActiveRecords(true)
									.setParameters(
											new Object[] { doc_basetype })
									.firstId();
							payment.setC_DocType_ID(payment_doctype_id);

							payment.setC_Currency_ID(imp.getC_Currency_ID());
							// payment.setC_ConversionType_ID(imp.getC_ConversionType_ID());
							// payment.setC_Charge_ID(imp.getC_Charge_ID());
							// payment.setChargeAmt(imp.getChargeAmt());
							// payment.setTaxAmt(imp.getTaxAmt());
							payment.setPayAmt(imp.getPaidAmt());

							// Save payment
							if (payment.save()) {
								order.setC_Payment_ID(payment.getC_Payment_ID());
								order.save();
								if (payment != null && m_docAction != null
										&& m_docAction.length() > 0) {
									payment.setDocAction(m_docAction);
									if (!payment.processIt(m_docAction)) {
										log.warning("Payment Process Failed: "
												+ payment + " - "
												+ payment.getProcessMsg());
										throw new IllegalStateException(
												"Payment Process Failed: "
														+ payment
														+ " - "
														+ payment
																.getProcessMsg());

									}
									payment.saveEx();
								}

							}
						}

						noInsert++;
						lineNo = 10;
					}
					imp.setC_Order_ID(order.getC_Order_ID());

					if (imp.getPriceActual().signum() != 0
							|| (imp.getDescription() != null && imp
									.getDescription().length() > 0)
							|| (imp.getC_Charge_ID() != 0 && imp
									.getM_Product_ID() != 0)) {
						// New OrderLine
						MOrderLine line = new MOrderLine(order);
						line.setLine(lineNo);
						lineNo += 10;
						if (imp.getM_Product_ID() != 0)
							line.setM_Product_ID(imp.getM_Product_ID(), true);
						if (imp.getC_Charge_ID() != 0)
							line.setC_Charge_ID(imp.getC_Charge_ID());
						line.setQty(imp.getQtyOrdered());
						line.setPrice();
						if (imp.getPriceActual().compareTo(Env.ZERO) != 0)
							line.setPrice(imp.getPriceActual());
						if (imp.getC_Tax_ID() != 0)
							line.setC_Tax_ID(imp.getC_Tax_ID());
						else {
							line.setTax();
							imp.setC_Tax_ID(line.getC_Tax_ID());
						}
						if (imp.getLineDescription() != null)
							line.setDescription(imp.getLineDescription());
						line.saveEx();
						imp.setC_OrderLine_ID(line.getC_OrderLine_ID());
					}
					imp.setI_IsImported(true);
					imp.setProcessed(true);
					//
					if (((PO) imp).save())
						noInsertLine++;
				}
				if (order != null) {
					if (m_docAction != null && m_docAction.length() > 0) {
						order.setDocAction(m_docAction);
						if (!order.processIt(m_docAction)) {
							log.warning("Order Process Failed: " + order
									+ " - " + order.getProcessMsg());
							throw new IllegalStateException(
									"Order Process Failed: " + order + " - "
											+ order.getProcessMsg());

						}
					}
					order.saveEx();
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Order - " + sql.toString(), e);
			} finally {
				DB.close(rs, pstmt);
				rs = null;
				pstmt = null;
			}
		}
		// Set Error indicator to not imported
		sql = new StringBuilder("UPDATE ").append(getM_TableName()).append(" ")
				.append("SET I_IsImported='N', Updated=SysDate ")
				.append("WHERE I_IsImported<>'Y'").append(clientCheck);
		no = DB.executeUpdate(sql.toString(), get_TrxName());
		addLog(0, null, new BigDecimal(no), "@Errors@");
		//
		addLog(0, null, new BigDecimal(noInsert), "@C_Order_ID@: @Inserted@");
		addLog(0, null, new BigDecimal(noInsertLine),
				"@C_OrderLine_ID@: @Inserted@");
		StringBuilder msgreturn = new StringBuilder("#").append(noInsert)
				.append("/").append(noInsertLine);
		return msgreturn.toString();
	} // doIt

} // ImportExtendedOrder
