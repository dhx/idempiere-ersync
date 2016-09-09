package at.dhx.adempiere.ersync.model;

import java.sql.ResultSet;

import org.adempiere.base.IModelFactory;
import org.compiere.model.PO;
import org.compiere.util.Env;

public class ERSyncModelFactory implements IModelFactory {

	@Override
	public Class<?> getClass(String tableName) {
		if (tableName.equals(X_I_Web_Order.Table_Name)) {
			return X_I_Web_Order.class;
		} else if (tableName.equals(X_I_POS_Order.Table_Name)) {
			return X_I_POS_Order.class;
		} else if (tableName.equals(X_I_Auto_Movement.Table_Name)) {
			return X_I_Auto_Movement.class;
		}
		return null;
	}

	@Override
	public PO getPO(String tableName, int Record_ID, String trxName) {
		if (tableName.equals(X_I_Web_Order.Table_Name)) {
			return new X_I_Web_Order(Env.getCtx(), Record_ID, trxName);
		} else if (tableName.equals(X_I_POS_Order.Table_Name)) {
			return new X_I_POS_Order(Env.getCtx(), Record_ID, trxName);
		} else if (tableName.equals(X_I_Auto_Movement.Table_Name)) {
			return new X_I_Auto_Movement(Env.getCtx(), Record_ID, trxName);
		}
		return null;
	}

	@Override
	public PO getPO(String tableName, ResultSet rs, String trxName) {
		if (tableName.equals(X_I_Web_Order.Table_Name)) {
			return new X_I_Web_Order(Env.getCtx(), rs, trxName);
		} else if (tableName.equals(X_I_POS_Order.Table_Name)) {
			return new X_I_POS_Order(Env.getCtx(), rs, trxName);
		} else if (tableName.equals(X_I_Auto_Movement.Table_Name)) {
			return new X_I_Auto_Movement(Env.getCtx(), rs, trxName);
		}
		return null;
	}

}
