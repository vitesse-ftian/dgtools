package com.vitessedata.phi.deepgreen;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "QueryBuilder" Node.
 *
 * @author VitesseData
 */

public class QueryBuilderNodeFactory 
        extends NodeFactory<QueryBuilderNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public QueryBuilderNodeModel createNodeModel() {
        return new QueryBuilderNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<QueryBuilderNodeModel> createNodeView(final int viewIndex,
            final QueryBuilderNodeModel nodeModel) {
    	return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new QueryBuilderNodeDialog(); 
    }

}

