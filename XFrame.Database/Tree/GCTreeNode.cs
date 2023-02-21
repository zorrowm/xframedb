using System.Collections.Generic;

namespace XFrame.Database.Tree
{
    public class GCTreeNode
    {
        public string labelName;
        public string ID;
        public object tag =null;
        public List<GCTreeNode> children;
    }
}
