package Element;

import UserClient.Transaction.TransactionManage;

import static config.MainType.NODETYPE;

/**
 * @author :jinkai
 * @date :Created in 2019/10/18 9:43
 * @description:
 * @modified By:
 * @version:
 */

public class NodeAgent extends Agent{

    public NodeAgent(TransactionManage transactionManage) {
        super(transactionManage);
    }

    @Override
    byte getType() {
        return NODETYPE;
    }
}
