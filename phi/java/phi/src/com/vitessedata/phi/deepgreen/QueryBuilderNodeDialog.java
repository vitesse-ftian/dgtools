package com.vitessedata.phi.deepgreen;

import java.awt.BorderLayout;
import java.awt.Dimension;

import javax.swing.BorderFactory;
import javax.swing.JEditorPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ScrollPaneConstants;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;

/**
 * <code>NodeDialog</code> for the "QueryBuilder" Node.
 * 
 * @author VitesseData
 */
public class QueryBuilderNodeDialog extends NodeDialogPane { 

	private final JEditorPane m_stmt = new JEditorPane("text", "");
	
    /**
     * SQL Query dialog with text box. 
     */
    protected QueryBuilderNodeDialog() {
        super();
        m_stmt.setPreferredSize(new Dimension(350, 200));
        m_stmt.setText("select * from $1");
        
        final JScrollPane scrollPane = new JScrollPane(m_stmt,
        		ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED,
        		ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        scrollPane.setBorder(BorderFactory.createTitledBorder(" SQL Statement "));
        JPanel configPanel = new JPanel(new BorderLayout());
        configPanel.add(scrollPane, BorderLayout.CENTER);
        super.addTab("Settings",  configPanel, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {
        DatabaseQueryConnectionSettings s = new DatabaseQueryConnectionSettings();
        try {
        	s.loadValidatedConnection(settings, getCredentialsProvider());
        } catch (InvalidSettingsException ex) {
        	// use settings as they are
        }
        String stmt = s.getQuery();
        if (stmt == null) {
        	stmt = "select * from $1";
        }
        m_stmt.setText(stmt); 
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
    	DatabaseQueryConnectionSettings s = new DatabaseQueryConnectionSettings();
    	s.setQuery(m_stmt.getText().trim());
    	s.saveConnection(settings);
    } 
}
