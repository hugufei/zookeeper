 /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a class that implements prefix matching for 
 * components of a filesystem path. the trie
 * looks like a tree with edges mapping to 
 * the component of a path.
 * example /ab/bc/cf would map to a trie
 *           /
 *        ab/
 *        (ab)
 *      bc/
 *       / 
 *      (bc)
 *   cf/
 *   (cf)
 */
// 字典树完成配额目录的增删查
// 将拥有对应配额属性的节点设置标记属性property
// 在zk中的目录结构为/zookeeper/quota/xxx(可以有多级目录)/zookeeper_limits
// PathTrie只是用来管理配额的,如果一个znode没用到配额，那么它就和PathTrie没有关系
// 看名字就知道是字典书的实现了，源码也基本是这个思路,不懂可以自己去找字典树相关资料


 /**
  * zk中的目录 /zookeeper/quota/a/b/c/zookeeper_limits 对应的PathTrie结构为:
  *           root
  *          /
  *         a
  *        /
  *       b
  *      /
  *     c
  *     /
  *     zookeeper_limits
  *
  * PathTrie中的root即相当于zk目录中的/zookeeper/quota,所以在下面的函数中，path是PathTrie中的path，不是zk中的path，即不会以"/zookeeper/quota"开头
  */

//

//
//
//
//
//
//
//
//    /
//
 public class PathTrie {
    /**
     * the logger for this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(PathTrie.class);
    
    /**
     * the root node of PathTrie
     */
    //成员变量，即字典树的根
    private final TrieNode rootNode ;
    
    static class TrieNode {
        // 属性，看源码表现,就是设置了配额的节点
        boolean property = false;
        // 记录子节点相对路径 与 TrieNode的mapping
        final HashMap<String, TrieNode> children;
        TrieNode parent = null;
        /**
         * create a trienode with parent
         * as parameter
         * @param parent the parent of this trienode
         */
        //构造时，设置parent
        private TrieNode(TrieNode parent) {
            children = new HashMap<String, TrieNode>();
            this.parent = parent;
        }
        
        /**
         * get the parent of this node
         * @return the parent node
         */
        TrieNode getParent() {
            return this.parent;
        }
        
        /**
         * set the parent of this node
         * @param parent the parent to set to
         */
        void setParent(TrieNode parent) {
            this.parent = parent;
        }
        
        /**
         * a property that is set 
         * for a node - making it 
         * special.
         */
        void setProperty(boolean prop) {
            this.property = prop;
        }
        
        /** the property of this
         * node 
         * @return the property for this
         * node
         */
        boolean getProperty() {
            return this.property;
        }
        /**
         * add a child to the existing node
         * @param childName the string name of the child
         * @param node the node that is the child
         */
        // 添加childName的相对路径到map,注意:在调用方 设置node的parent
        void addChild(String childName, TrieNode node) {
            synchronized(children) {
                if (children.containsKey(childName)) {
                    return;
                }
                children.put(childName, node);
            }
        }
     
        /**
         * delete child from this node
         * @param childName the string name of the child to 
         * be deleted
         */
        //删除子节点
        void deleteChild(String childName) {
            synchronized(children) {
                if (!children.containsKey(childName)) {
                    return;
                }
                TrieNode childNode = children.get(childName);
                // this is the only child node.
                // 如果这个儿子只有1个儿子,那么就把这个儿子丢掉
                if (childNode.getChildren().length == 1) {
                    //被删除的子节点,parent设置为空
                    childNode.setParent(null);
                    children.remove(childName);
                }
                else {
                    // their are more child nodes
                    // so just reset property.
                    // 否则这个儿子还有其他儿子，标记它不是没有配额限制,这里有个bug，就是数量为0时，也进入这个逻辑
                    childNode.setProperty(false);
                }
            }
        }
        
        /**
         * return the child of a node mapping
         * to the input childname
         * @param childName the name of the child
         * @return the child of a node
         */
        // 根据childName从map中取出对应的TrieNode
        TrieNode getChild(String childName) {
            synchronized(children) {
               if (!children.containsKey(childName)) {
                   return null;
               }
               else {
                   return children.get(childName);
               }
            }
        }

        /**
         * get the list of children of this 
         * trienode.
         * @param node to get its children
         * @return the string list of its children
         */
        // 获取子节点String[]列表
        String[] getChildren() {
           synchronized(children) {
               return children.keySet().toArray(new String[0]);
           }
        }
        
        /**
         * get the string representation
         * for this node
         */
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Children of trienode: ");
            synchronized(children) {
                for (String str: children.keySet()) {
                    sb.append(" " + str);
                }
            }
            return sb.toString();
        }
    }
    
    /**
     * construct a new PathTrie with
     * a root node of /
     */
    public PathTrie() {
        this.rootNode = new TrieNode(null);
    }
    
    /**
     * add a path to the path trie 
     * @param path
     */
    // 字典树的增，这里就是把一个path按照/符号分开，加入字典树
    // 注意：path是PathTrie中的path，不是zk中的path，即不会以"/zookeeper/quota"开头
    public void addPath(String path) {
        if (path == null) {
            return;
        }
        //将路径按照"/" split开
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        //从1开始因为路径都是"/"开头的,pathComponents[0]会是""
        for (int i=1; i<pathComponents.length; i++) {
            part = pathComponents[i];
            if (parent.getChild(part) == null) {
                //一方面parent将这个child记录在map，一方面将这个child node进行初始化以及设置parent
                parent.addChild(part, new TrieNode(parent));
            }
            //进入到对应的child
            parent = parent.getChild(part);
        }
        //最后这个节点设置配额属性
        parent.setProperty(true);
    }
    
    /**
     * delete a path from the trie
     * @param path the path to be deleted
     */
    // 字典树的删
    // 说明： deletePath传入的参数,并不会一定精确到叶子节点,也就是可能会到某个目录，再调用realParent.deleteChild进行TrieNode的deleteChild操作
    public void deletePath(String path) {
        if (path == null) {
            return;
        }
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        String part = null;
        if (pathComponents.length <= 1) { 
            throw new IllegalArgumentException("Invalid path " + path);
        }
        for (int i=1; i<pathComponents.length; i++) {
            part = pathComponents[i];
            if (parent.getChild(part) == null) {
                //the path does not exist 
                return;
            }
            parent = parent.getChild(part);
            LOG.info("{}",parent);
        }
        // 得到被删除TrieNode的parent
        TrieNode realParent  = parent.getParent();
        // 将这个node从parent的子列表中删除
        realParent.deleteChild(part);
    }
    
    /**
     * return the largest prefix for the input path.
     * @param path the input path
     * @return the largest prefix for the input path.
     */
    // 字典树的查
    // 这个函数的作用就是根据path 按路径远近 找到最近的拥有配额标记的节点的路径
    // 实现方式：
    // 1) 将path按/分开，进行字典树查找，从根开始
    // 2) 记住路径中最后一个拥有配额标记的TrieNode
    //找到最近的一个拥有配额标记的祖先节点
    public String findMaxPrefix(String path) {
        if (path == null) {
            return null;
        }
        if ("/".equals(path)) {
            return path;
        }
        //按照/分开
        String[] pathComponents = path.split("/");
        TrieNode parent = rootNode;
        List<String> components = new ArrayList<String>();
        if (pathComponents.length <= 1) {
            throw new IllegalArgumentException("Invalid path " + path);
        }
        //因为路径是/开头
        int i = 1;
        String part = null;
        StringBuilder sb = new StringBuilder();
        int lastindex = -1;
        while((i < pathComponents.length)) {
            if (parent.getChild(pathComponents[i]) != null) {
                part = pathComponents[i];
                //一层层到子节点
                parent = parent.getChild(part);
                components.add(part);
                //如果对应的子节点有标记
                if (parent.getProperty()) {
                    //更新最后一个有标记的节点(也就是最近的有标记的祖先)
                    lastindex = i-1;
                }
            }
            else {
                break;
            }
            i++;
        }
        for (int j=0; j< (lastindex+1); j++) {
            sb.append("/" + components.get(j));
        }
        //返回最近的,有标记的祖先的路径
        return sb.toString();
    }

    /**
     * clear all nodes
     */
    public void clear() {
        for(String child : rootNode.getChildren()) {
            rootNode.deleteChild(child);
        }
    }
}
