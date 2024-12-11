import math

class BTreeNode:
    def __init__(self, order, is_leaf=False):
        self.keys = []  # The keys stored in the node
        self.values = []  # Pointers to the data
        self.children = []  # Child pointers
        self.is_leaf = is_leaf  # Is a leaf node?
        self.next = None  # Pointer to the next leaf node (for range queries)
        self.order = order  # Maximum number of children
        self.parent = None
        self.min_children = math.ceil(order / 2) 
        self.min_key = math.ceil(order / 2) - 1

    def is_full(self):
        return len(self.keys) >= self.order - 1


class BPlusTree:
    def __init__(self, order=4):
        self.root = BTreeNode(order, is_leaf=True)
        self.order = order

    def insert(self, key, value):
        result = self._insert_recursive(self.root, key, value)
        if isinstance(result, tuple):  # Root was split
            middle_key, new_node = result
            new_root = BTreeNode(self.order, is_leaf=False)
            new_root.keys = [middle_key]
            new_root.children = [self.root, new_node]

            # Update parent references
            self.root.parent = new_root
            new_node.parent = new_root

            self.root = new_root

    def _insert_recursive(self, node, key, value):
        if node.is_leaf:
            if key in node.keys:
                index = node.keys.index(key)
                if isinstance(node.values[index], list):
                    node.values[index].append(value)
                else:
                    node.values[index] = [node.values[index], value]
            else:
                node.keys.append(key)
                node.values.append(value)

                # Ensure keys and values remain sorted
                sorted_indices = sorted(range(len(node.keys)), key=lambda i: node.keys[i])
                node.keys = [node.keys[i] for i in sorted_indices]
                node.values = [node.values[i] for i in sorted_indices]
        else:
            index = self._find_child_index(node, key)
            child_result = self._insert_recursive(node.children[index], key, value)

            if isinstance(child_result, tuple):
                middle_key, new_node = child_result
                node.keys.insert(index, middle_key)
                node.children.insert(index + 1, new_node)

                # Update parent references
                new_node.parent = node
                for child in node.children:
                    child.parent = node

        if len(node.keys) > self.order - 1:
            return self._split_node(node)

        return node

    def _split_node(self, node):
        mid = len(node.keys) // 2
        new_node = BTreeNode(self.order, is_leaf=node.is_leaf)

        if node.is_leaf:
            # Split leaf node
            new_node.keys = node.keys[mid:]
            new_node.values = node.values[mid:]
            middle_key = node.keys[mid]
            node.keys = node.keys[:mid]
            node.values = node.values[:mid]

            # Update leaf chain
            new_node.next = node.next
            node.next = new_node
        else:
            # Split internal node
            new_node.keys = node.keys[mid + 1:]
            new_node.children = node.children[mid + 1:]
            middle_key = node.keys[mid]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid + 1]

            # Update parent for new_node's children
            for child in new_node.children:
                child.parent = new_node

        # Set parent for the new node
        new_node.parent = node.parent

        return middle_key, new_node

    def _find_child_index(self, node, key):
        for i, k in enumerate(node.keys):
            if key < k:
                return i
        return len(node.keys)

    def _find_child_index_deletion(self, node, key):
        for i, k in enumerate(node.keys):
            if key < k and node.keys[i - 1] != key :
                return i, -999
            if key < k and node.keys[i - 1] == key : 
                return i, -1000
        return len(node.keys), -1001

    def search(self, key):
        return self._search_recursive(self.root, key)

    def _search_recursive(self, node, key):
        if node.is_leaf:
            if key in node.keys:
                index = node.keys.index(key)
                return node.values[index]
            return None
        else:
            index = self._find_child_index(node, key)
            return self._search_recursive(node.children[index], key)

    def search_range(self, start, end):
        current_leaf = self.root
        while not current_leaf.is_leaf:
            for i, key in enumerate(current_leaf.keys):
                if start < key:
                    current_leaf = current_leaf.children[i]
                    break
            else:
                current_leaf = current_leaf.children[-1]
        
        result = []
        while current_leaf:
            for i, key in enumerate(current_leaf.keys):
                if start <= key <= end:
                    result.extend(current_leaf.values[i] if isinstance(current_leaf.values[i], list) else [current_leaf.values[i]])
                elif key > end:
                    return result
            current_leaf = current_leaf.next
        
        return result

    def print_tree(self, node=None, prefix="", is_last=True):
        if node is None:
            node = self.root

        print(prefix, end="")
        print("└── " if is_last else "├── ", end="")
        
        node_type = "L" if node.is_leaf else "I"
        print(f"{node_type}: {node.keys} | {node.values if node.is_leaf else ''}")

        if not node.is_leaf:
            new_prefix = prefix + ("    " if is_last else "│   ")
            for i in range(len(node.children)):
                is_last_child = (i == len(node.children) - 1)
                self.print_tree(
                    node=node.children[i], 
                    prefix=new_prefix, 
                    is_last=is_last_child
                )

    def print_leaf_chain(self):
        print("\nLeaf Chain:")
        current_leaf = self.root
        while not current_leaf.is_leaf:
            current_leaf = current_leaf.children[0]
        while current_leaf:
            print(f"[{current_leaf.keys}] → ", end="")
            current_leaf = current_leaf.next
        print("None")
        
    def delete(self, key):
        if not self.root:
            return

        print(f"Deleting key: {key}")
        self._delete_recursive(self.root, key)

        # If the root becomes empty and is not a leaf, promote its only child
        if not self.root.keys and not self.root.is_leaf:
            self.root = self.root.children[0]

    def _delete_recursive(self, node, key):
        if node.is_leaf:
            # Delete from leaf node
            if key in node.keys:
                index = node.keys.index(key)
                print("key =", key)
                print("index yg akan dihapus =", index)
                print("yang akan dipop=", node.keys, "=", node.keys[index])
                node.keys.pop(index)
                node.values.pop(index)
            else:
                print(f"Key {key} not found in the tree.")
                return
        else:
            # Find child to recurse into
            index = self._find_child_index(node, key)
            child = node.children[index]

            if key in node.keys:
                # SHOULD I USE THIS CONDITIONAL? 
                # if len(child.keys) > child.min_key :
                    # Key is in the internal node
                self._delete_recursive(child, key)
                successor = self._find_successor(child)
                node.keys[index - 1] = successor
                # else : 
                #     self._rebalance_leaf(child)
                # so that it would be like this
            else:
                # Recurse into the child
                self._delete_recursive(child, key)

        # Handle underflow after deletion
        self._handle_underflow(node)

    def _handle_underflow(self, node):
        if node.is_leaf:
            # Underflow condition for leaf nodes
            if len(node.keys) < node.min_key:
                self._rebalance_leaf(node)
        else:
            # Underflow condition for internal nodes
            if len(node.keys) < node.min_children - 1:
                self._rebalance_internal(node)

    def _rebalance_leaf(self, node):
        parent = node.parent
        if not parent:
            return

        index = parent.children.index(node)
        left_sibling = parent.children[index - 1] if index > 0 else None
        right_sibling = parent.children[index + 1] if index < len(parent.children) - 1 else None

        if left_sibling and len(left_sibling.keys) > left_sibling.min_key:
            # Borrow from left sibling
            node.keys.insert(0, left_sibling.keys.pop(-1))
            node.values.insert(0, left_sibling.values.pop(-1))
            parent.keys[index - 1] = node.keys[0]
        elif right_sibling and len(right_sibling.keys) > right_sibling.min_key:
            # Borrow from right sibling
            node.keys.append(right_sibling.keys.pop(0))
            node.values.append(right_sibling.values.pop(0))
            parent.keys[index] = right_sibling.keys[0]
        else: 
            # Merge with a sibling
            if left_sibling:
                left_sibling.keys.extend(node.keys)
                left_sibling.values.extend(node.values)
                left_sibling.next = node.next
                parent.keys.pop(index - 1)
                parent.children.pop(index)
            elif right_sibling:
                node.keys.extend(right_sibling.keys)
                node.values.extend(right_sibling.values)
                node.next = right_sibling.next
                parent.keys.pop(index)
                parent.children.pop(index + 1)

    def _rebalance_internal(self, node):
        parent = node.parent
        if not parent:
            return

        index = parent.children.index(node)
        left_sibling = parent.children[index - 1] if index > 0 else None
        right_sibling = parent.children[index + 1] if index < len(parent.children) - 1 else None

        if left_sibling and len(left_sibling.keys) > left_sibling.min_key:
            # Borrow from left sibling
            node.keys.insert(0, parent.keys[index - 1])
            node.children.insert(0, left_sibling.children.pop(-1))
            parent.keys[index - 1] = left_sibling.keys.pop(-1)
        elif right_sibling and len(right_sibling.keys) > right_sibling.min_key:
            # Borrow from right sibling
            node.keys.append(parent.keys[index])
            node.children.append(right_sibling.children.pop(0))
            parent.keys[index] = right_sibling.keys.pop(0)
        else:
            # Merge with a sibling
            if left_sibling:
                left_sibling.keys.append(parent.keys.pop(index - 1))
                left_sibling.keys.extend(node.keys)
                left_sibling.children.extend(node.children)
                parent.children.pop(index)
            elif right_sibling:
                node.keys.append(parent.keys.pop(index))
                node.keys.extend(right_sibling.keys)
                node.children.extend(right_sibling.children)
                parent.children.pop(index + 1)

    def _find_successor(self, node):
        current = node
        while not current.is_leaf:
            current = current.children[0]
        return current.keys[0]