package com.vitessedata.phi.deepgreen;

import org.knime.base.node.io.database.DBNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.reader.DBReader;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;


/**
 * This is the model implementation of QueryBuilder.
 *
 * @author VitesseData
 */
public class QueryBuilderNodeModel extends DBNodeModel { 
    
    // the logger instance
    // private static final NodeLogger logger = NodeLogger.getLogger(QueryBuilderNodeModel.class);
	private String m_query = "SELECT * FROM $1";

	QueryBuilderNodeModel() {
		super(new PortType[]{DatabasePortObject.TYPE, DatabasePortObject.TYPE_OPTIONAL}, 
				new PortType[]{DatabasePortObject.TYPE});
	}

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        super.saveSettingsTo(settings);
        settings.addString(DatabaseConnectionSettings.CFG_STATEMENT, m_query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        super.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        super.loadValidatedSettingsFrom(settings);
        m_query = settings.getString(DatabaseConnectionSettings.CFG_STATEMENT);
    }
    

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs)
        throws InvalidSettingsException {
        DatabasePortObjectSpec spec = (DatabasePortObjectSpec) inSpecs[0];
        DatabaseQueryConnectionSettings conn = spec.getConnectionSettings(getCredentialsProvider());
        String q1 = conn.getQuery();
        
        String q2 = null;
        if (inSpecs[1] != null) {
        	DatabasePortObjectSpec spec2 = (DatabasePortObjectSpec) inSpecs[1];
        	DatabaseQueryConnectionSettings conn2 = spec2.getConnectionSettings(getCredentialsProvider());
        	q2 = conn2.getQuery();
        }

        String newQuery = buildQuery(q1, q2); 
        conn = createDBQueryConnection(spec, newQuery);

        if (!conn.getRetrieveMetadataInConfigure()) {
        	return new PortObjectSpec[1];
        }

        try {
        	DBReader reader = conn.getUtility().getReader(conn);
        	DataTableSpec outSpec = reader.getDataTableSpec(
        			getCredentialsProvider());
        	DatabasePortObjectSpec dbSpec = new DatabasePortObjectSpec(
        			outSpec, conn.createConnectionModel());
        	return new PortObjectSpec[]{dbSpec};
        } catch (Throwable t) {
        	throw new InvalidSettingsException(t);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected final PortObject[] execute(final PortObject[] inData,
            final ExecutionContext exec)
            throws CanceledExecutionException, Exception {
        DatabasePortObject dbObj = (DatabasePortObject) inData[0];
        DatabaseQueryConnectionSettings conn = dbObj.getConnectionSettings(getCredentialsProvider());
        String q1 = conn.getQuery();
        String q2 = null;
        if (inData[1] != null) {
        	DatabasePortObject dbObj2 = (DatabasePortObject) inData[1];
        	DatabaseQueryConnectionSettings conn2 = dbObj2.getConnectionSettings(getCredentialsProvider());
        	q2 = conn2.getQuery();
        }
        
        String newQuery = buildQuery(q1, q2); 
        conn = createDBQueryConnection(dbObj.getSpec(), newQuery);
        DBReader load = conn.getUtility().getReader(conn);
        DataTableSpec outSpec = load.getDataTableSpec(
                getCredentialsProvider());
        DatabasePortObjectSpec dbSpec = new DatabasePortObjectSpec(
                outSpec, conn.createConnectionModel());
        DatabasePortObject outObj = new DatabasePortObject(dbSpec);
        return new PortObject[]{outObj};
    }

    private static long m_cnt = 1;
    private String nextTmp() {
    	String t = "KNIME_QBWT_" + m_cnt;
    	m_cnt += 1;
    	return t;
    }

    private String buildQuery(String q1, String q2) {
    	StringBuilder sb = new StringBuilder();
    	String tmp1 = nextTmp();
    	String tmp2 = nextTmp();
    	sb.append("WITH ").append(tmp1).append(" AS (\n");
    	sb.append(q1);
    	if (q2 != null && !q2.trim().isEmpty()) {
    		sb.append(" ),\n").append(tmp2).append(" AS (\n");
    		sb.append(q2);
    	}
    	sb.append(")\n");
    	
    	String replace1 = m_query.replaceAll("[$]1", tmp1);
    	String replace2 = replace1.replaceAll("[$]2", tmp2);
    	sb.append(replace2);
    	return sb.toString();
    }
}