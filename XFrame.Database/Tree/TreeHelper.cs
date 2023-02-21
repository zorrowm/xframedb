using System.Collections.Generic;

namespace XFrame.Database.Tree
{
    public class TreeHelper
    {
        /// <summary>
        /// 递归方式构造树
        /// </summary>
        /// <param name="parentNode">父节点</param>
        /// <param name="parentID">父ID</param>
        /// <param name="childID">ID</param>
        /// <param name="childName">显示标记</param>
        /// <param name="pTag">附加对象</param>
        /// <returns></returns>
        public static  bool constructTree(GCTreeNode parentNode, string parentID, string childID, string childName, object pTag)
        {
            if (parentID.Equals(parentNode.ID))
            {
                if (parentNode.children == null)
                    parentNode.children = new List<GCTreeNode>();
                GCTreeNode childNode = new GCTreeNode();
                childNode.labelName = childName;
                childNode.ID = childID;
                childNode.tag = pTag;
                parentNode.children.Add(childNode);
                return true;
            }
            else
            {
                if (parentNode.children != null)
                {
                    bool result = false;
                    foreach (GCTreeNode tmpNode in parentNode.children)
                    {
                        result = constructTree(tmpNode, parentID, childID, childName, pTag);
                        if (result)
                            return true;
                    }
                }
            }
            return false;
        }
    }
}
