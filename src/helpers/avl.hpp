/**
 * @file avl.hpp
 * @author Ashot Vardanian
 * @brief A custom binary-search tree for in-memory DB.
 * @version 0.1
 * @date 2022-09-28
 *
 * @section Why not `std::map`?
 * There are a few aspects that the tree should meet:
 * 1. concurrent on writes, not just reads with `std::shared_mutex`.
 * 2. random sampling requires explicit access to subtree sizes.
 * The first issue can be used by having a composition of multiple
 * containers each under its shared lock, while the second would anyways
 * require explicit acceess to the container internals.
 *
 */

#pragma once
#include <algorithm> // `std::max`

namespace unum::ukv {

/**
 * @brief AVL-Trees are some of the simplest yet performant Binary Search Trees.
 * This "node" class implements the primary logic, but doesn't take part in memory
 * management.
 *
 * @tparam element_at
 * @tparam comparator_at
 * @tparam node_allocator_at
 */
template <typename element_at, typename comparator_at, typename node_allocator_at>
class avl_node_gt {
  public:
    using element_t = element_at;
    using comparator_t = comparator_at;
    using node_allocator_t = node_allocator_at;
    using height_t = std::int16_t;
    using node_t = avl_node_gt;

    element_t element;
    node_t* left = nullptr;
    node_t* right = nullptr;
    height_t height = 0;

    static height_t get_height(node_t* node) noexcept { return node ? node->height : 0; }
    static height_t get_balance(node_t* node) noexcept {
        return node ? get_height(node->left) - get_height(node->right) : 0;
    }

    static node_t* new_node(element_t element) noexcept {
        node_t* node = new node_t();
        node->element = element;
        node->left = nullptr;
        node->right = nullptr;
        node->height = 1;
        return node;
    }

    static void delete_node(node_t* node) noexcept { delete node; }

#pragma mark - Search

    static node_t* find_min(node_t* node) noexcept {
        while (node->left)
            node = node->left;
        return node;
    }

    static node_t* find_max(node_t* node) noexcept {
        while (node->right)
            node = node->right;
        return node;
    }

    /**
     * @brief Searches for equal element in this subtree.
     * @param comparable Any key comparable with stored elements.
     * @return NULL if nothing was found.
     */
    template <typename comparable_at>
    static node_t* find(node_t* node, comparable_at&& comparable) noexcept {
        auto less = comparator_t {};
        while (node) {
            if (less(comparable, node->element))
                node = node->left;
            else if (less(node->element, comparable))
                node = node->right;
            else
                break;
        }
        return node;
    }

    /**
     * @brief Find the smallest element, bigger than the provided one.
     * @param comparable Any key comparable with stored elements.
     * @return NULL if nothing was found.
     *
     * Is used for an atomic implementation of iterators.
     * Alternatively one can:
     * > store a stack for path, which is ~O(logN) space.
     * > store parents in nodes and have complex logic.
     * This implementation has no recursion and no
     */
    template <typename comparable_at>
    static node_t* find_successor(node_t* node, comparable_at&& comparable) noexcept {
        node_t* succ = nullptr;
        auto less = comparator_t {};
        while (node) {
            // If the given key is less than the root node, visit the left subtree,
            // taking current node as potential successor.
            if (less(comparable, node->element)) {
                succ = node;
                node = node->left;
            }

            // Of the given key is more than the root node, visit the right subtree.
            else if (less(node->element, comparable)) {
                node = node->right;
            }

            // If a node with the desired value is found, the successor is the minimum
            // value node in its right subtree (if any).
            else {
                if (node->right)
                    succ = find_min(node->right);
                break;
            }
        }
        return succ;
    }

#pragma mark - Insertions

    static node_t* rotate_right(node_t* y) noexcept {
        node_t* x = y->left;
        node_t* z = x->right;

        // Perform rotation
        x->right = y;
        y->left = z;

        // Update heights
        y->height = std::max(get_height(y->left), get_height(y->right)) + 1;
        x->height = std::max(get_height(x->left), get_height(x->right)) + 1;
        return x;
    }

    static node_t* rotate_left(node_t* x) noexcept {
        node_t* y = x->right;
        node_t* z = y->left;

        // Perform rotation
        y->left = x;
        x->right = z;

        // Update heights
        x->height = std::max(get_height(x->left), get_height(x->right)) + 1;
        y->height = std::max(get_height(y->left), get_height(y->right)) + 1;
        return y;
    }

    struct find_or_make_result_t {
        node_t* root = nullptr;
        node_t* match = nullptr;
        bool inserted = false;
    };

    inline static node_t* rebalance_after_insert(node_t* node) noexcept {
        // Update height and check if branches aren't balanced
        node->height = std::max(get_height(node->left), get_height(node->right)) + 1;
        auto balance = get_balance(node);
        auto less = comparator_t {};

        // Left Left Case
        if (balance > 1 && less(element, node->left->element))
            return rotate_right(node);

        // Right Right Case
        else if (balance < -1 && less(node->right->element, element))
            return rotate_left(node);

        // Left Right Case
        else if (balance > 1 && less(node->left->element, element)) {
            node->left = rotate_left(node->left);
            return rotate_right(node);
        }
        // Right Left Case
        else if (balance < -1 && less(element, node->right->element)) {
            node->right = rotate_right(node->right);
            return rotate_left(node);
        }
        else
            return node;
    }

    template <typename comparable_at, typename node_allocator_at>
    static find_or_make_result_t find_or_make(node_t* node,
                                              comparable_at&& comparable,
                                              node_allocator_at&& node_allocator) noexcept {
        if (!node) {
            node = new_node(comparable);
            return {node, node, true};
        }

        auto less = comparator_t {};
        if (less(comparable, node->element)) {
            auto downstream = find_or_make(node->left, comparable, node_allocator);
            node->left = downstream.root;
            if (downstream.inserted)
                node = rebalance_after_insert(node);
            return {node, downstream.match, downstream.inserted};
        }
        else if (less(node->element, comparable)) {
            auto downstream = find_or_make(node->right, comparable, node_allocator);
            node->right = downstream.root;
            if (downstream.inserted)
                node = rebalance_after_insert(node);
            return {node, downstream.match, downstream.inserted};
        }
        else {
            // Equal keys are not allowed in BST
            return {node, node, false};
        }
    }

    template <typename node_allocator_at>
    static node_t* insert(node_t* node, element_t element) noexcept {
        auto result = find_or_make(node, element, node_allocator);
        if (result.inserted)
            result.match->element = std::move(element);
        return result.root;
    }

#pragma mark - Removals

    struct pop_result_t {
        node_t* root = nullptr;
        std::unique_ptr<node_t> popped;
    };

    inline static node_t* rebalance_after_pop(node_t* node) noexcept {
        node->height = 1 + std::max(get_height(node->left), get_height(node->right));
        auto balance = get_balance(node);

        // Left Left Case
        if (balance > 1 && get_balance(node->left) >= 0)
            return rotate_right(node);

        // Left Right Case
        else if (balance > 1 && get_balance(node->left) < 0) {
            node->left = rotate_left(node->left);
            return rotate_right(node);
        }

        // Right Right Case
        else if (balance < -1 && get_balance(node->right) <= 0)
            return rotate_left(node);

        // Right Left Case
        else if (balance < -1 && get_balance(node->right) > 0) {
            node->right = rotate_right(node->right);
            return rotate_left(node);
        }
        else
            return node;
    }

    template <typename comparable_at>
    static pop_result_t pop(node_t* node, comparable_at&& comparable) noexcept {
        if (!node)
            return {node, {}};

        auto less = comparator_t {};
        if (less(comparable, node->element)) {
            auto downstream = pop(node->left, element);
            node->left = downstream.node;
            if (downstream.popped)
                node = rebalance_after_pop(node);
            return {node, std::move(downstream.popped)};
        }

        else if (less(node->element, comparable)) {
            auto downstream = pop(node->right, element);
            node->right = downstream.node;
            if (downstream.popped)
                node = rebalance_after_pop(node);
            return {node, std::move(downstream.popped)};
        }

        // We have found the node to pop!
        else {
            // If the node has two children, replace it with the
            // smallest entry in the right branch.
            if (node->left && node->right) {
                node_t* midpoint = find_min(node->right);
                auto downstream = pop(midpoint->right, midpoint->element);
                midpoint = downstream.popped.release();
                midpoint->left = node->left;
                midpoint->right = downstream.root;
                // Detach the `node` from the descendants.
                node->left = node->right = nullptr;
                return {midpoint, {node}};
            }
            // Just one child is present, so it is the natural successor.
            else if (node->left || node->right) {
                node_t* replacement = node->left ? node->left : node->right;
                // Detach the `node` from the descendants.
                node->left = node->right = nullptr;
                return {replacement, {node}};
            }
            // No children are present.
            else {
                // Detach the `node` from the descendants.
                node->left = node->right = nullptr;
                return {nullptr, {node}};
            }
        }
    }

    struct remove_if_result_t {
        node_t* root = nullptr;
        std::size_t count = 0;
    };

    template <typename predicate_at>
    static remove_if_result_t remove_if(node_t* node, predicate_at&& predicate) noexcept {
        if (!node)
            return node;
    }
};

template <typename element_at,
          typename comparator_at,
          typename node_allocator_at = std::allocator<avl_node_gt<element_at, comparator_at>>>
class avl_tree_gt {
  public:
    using node_t = avl_node_gt<element_at, comparator_at>;

  private:
    node_t* root_ = nullptr;

  public:
    template <typename comparable_at>
    node_t* find(comparable_at&& comparable) noexcept {
        return node_t::find(root_, comparable);
    }

    template <typename comparable_at>
    node_t* find_successor(comparable_at&& comparable) noexcept {
        return node_t::find_successor(root_, comparable);
    }

    struct node_element_ref_t {
        node_t* node = nullptr;
    };

    node_element_ref_t insert(comparable_at&& comparable) {}
};

template <typename key_at, typename value_at, typename key_comparator_at, typename node_allocator_at>
class acid_kvs_gt {
  public:
    using key_t = key_at;
    using value_t = value_at;
    using generation_t = std::size_t;
    using key_comparator_t = key_comparator_at;

    struct entry_t {
        key_t key;
        generaton_t generation = 0;
        value_t value;
    };

    struct entry_comparator_t {
        bool operator()(key_t const& a, key_t const& b) const noexcept { return key_comparator_t {}(a, b); }
        bool operator()(entry_t const& a, entry_t const& b) const noexcept {
            auto is_less = key_comparator_t {}(a.key, b.key);
            auto is_more = key_comparator_t {}(b.key, a.key);
            return is_less || is_more ? is_less : a.generation < b.generation;
        }
    };

    using tree_t = avl_tree_gt<entry_t, entry_comparator_t, node_allocator_at>;

    class transaction_t {
      public:
        bool get(key_t const&, value_t& value) noexcept;
        bool set(key_t const&, value_t const& value) noexcept;
    };

    class tree_t {
        std::shared_mutex mutex;
        tree_t tree;
    };

    bool get(key_t const&, value_t& value) noexcept;
    bool set(key_t const&, value_t const& value) noexcept;
};

} // namespace unum::ukv
