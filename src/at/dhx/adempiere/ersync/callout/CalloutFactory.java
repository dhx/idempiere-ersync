/**
 * 
 */
package at.dhx.adempiere.ersync.callout;

import java.util.ArrayList;
import java.util.List;

import org.adempiere.base.IColumnCallout;
import org.adempiere.base.IColumnCalloutFactory;
import org.compiere.model.MMovementLine;

import at.dhx.adempiere.ersync.model.I_I_Auto_Movement;

/**
 * @author dhx
 *
 */
public class CalloutFactory implements IColumnCalloutFactory {

	/* (non-Javadoc)
	 * @see org.adempiere.base.IColumnCalloutFactory#getColumnCallouts(java.lang.String, java.lang.String)
	 */
	@Override
	public IColumnCallout[] getColumnCallouts(String tableName,
			String columnName) {
		List<IColumnCallout> list = new ArrayList<IColumnCallout>();
		
		if (tableName.equals(I_I_Auto_Movement.Table_Name) && (
				columnName.equals(I_I_Auto_Movement.COLUMNNAME_M_Product_ID)
				|| columnName.equals(I_I_Auto_Movement.COLUMNNAME_ProductValue)
			 	))
			list.add(new CalloutFromFactory());

		return list != null ? list.toArray(new IColumnCallout[0]) : new IColumnCallout[0];
	}

}
