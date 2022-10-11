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
 * require explicit access to the container internals.
 *
 */

#pragma once
#include <algorithm> // `std::max`
#include <memory>    // `std::allocator`

namespace unum::ukv {

/**
 * @brief AVL-Trees are some of the simplest yet performant Binary Search Trees.
 * This "node" class implements the primary logic, but doesn't take part in memory
 * management.
 *
 * > Never throws! Even if new node allocation had failed.
 * > Implements `find_successor` for faster and lighter iterators.
 *   Alternative would be - Binary Threaded Search Tree,
 * > Implements sampling methods.
 *
 * @tparam element_at       Type of elements to store in this tree.
 * @tparam comparator_at    A comparator function object, that overload
 *                          @code
 *                              bool operator ()(element_at, element_at) const
 *                          @endcode
 */
template <typename element_at, typename comparator_at>
class avl_node_gt {
  public:
    using element_t = element_at;
    using comparator_t = comparator_at;
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

#pragma mark - Search

    template <typename callback_at>
    static void for_each(node_t* node, callback_at&& callback) noexcept {
        if (!node)
            return;
        callback(node);
        for_each(node->left, callback);
        for_each(node->right, callback);
    }

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
     * @brief Searches for the shortest node, that is ancestor of both provided keys.
     * @return NULL if nothing was found.
     * ! Recursive implementation is suboptimal.
     */
    template <typename comparable_a_at, typename comparable_b_at>
    static node_t* lowest_common_ancestor(node_t* node, comparable_a_at&& a, comparable_b_at&& b) noexcept {
        if (!node)
            return nullptr;

        auto less = comparator_t {};
        // If both `a` and `b` are smaller than `node`, then LCA lies in left
        if (less(a, node->element) && less(b, node->element))
            return lowest_common_ancestor(node->left, a, b);

        // If both `a` and `b` are greater than `node`, then LCA lies in right
        else if (less(node->element, a) && less(node->element, b))
            return lowest_common_ancestor(node->right, a, b);

        else
            return node;
    }

    /**
     * @brief Searches for the first/smallest element that compares equal to the provided element.
     */
    template <typename comparable_at>
    static node_t* lower_bound(node_t* node, comparable_at&& comparable) noexcept;

    /**
     * @brief Searches for the last/biggest element that compares equal to the provided element.
     */
    template <typename comparable_at>
    static node_t* upper_bound(node_t* node, comparable_at&& comparable) noexcept;

    struct node_range_t {
        node_t* lower_bound = nullptr;
        node_t* upper_bound = nullptr;
        node_t* lowest_common_ancestor = nullptr;
    };

    /**
     * @brief Complex method, that detects the left-most and right-most nodes
     * containing keys in a provided ranges, as well as their lowest common ancestors.
     * ! Has a recursive implementation for now.
     */
    template <typename lower_at, typename upper_at, typename callback_at>
    static node_range_t find_range(node_t* node, lower_at&& low, upper_at&& high, callback_at&& callback) noexcept {
        if (!node)
            return {};

        // If this node fits into the range - analyze its children.
        // The first call to reach this branch in the call-stack
        // will be by definition the Lowest Common Ancestor.
        auto less = comparator_t {};
        if (!less(high, node->element) && !less(node->element, low)) {
            callback(node);
            auto left_subrange = find_range(node->left, low, high, callback);
            auto right_subrange = find_range(node->right, low, high, callback);

            auto result = node_range_t {};
            result.lower_bound = left_subrange.lower_bound ?: node;
            result.upper_bound = right_subrange.upper_bound ?: node;
            result.lowest_common_ancestor = node;
            return result;
        }

        else if (less(node->element, low))
            return find_range(node->right, low, high, callback);

        else
            return find_range(node->left, low, high, callback);
    }

    template <typename comparable_at>
    static node_range_t equal_range(node_t* node, comparable_at&& comparable) noexcept {
        return find_range(node, comparable, comparable);
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

        /**
         * @return True if the allocation of the new node has failed.
         */
        bool failed() const noexcept { return inserted && !match; }
    };

    template <typename comparable_at>
    inline static node_t* rebalance_after_insert(node_t* node, comparable_at&& comparable) noexcept {
        // Update height and check if branches aren't balanced
        node->height = std::max(get_height(node->left), get_height(node->right)) + 1;
        auto balance = get_balance(node);
        auto less = comparator_t {};

        // Left Left Case
        if (balance > 1 && less(comparable, node->left->element))
            return rotate_right(node);

        // Right Right Case
        else if (balance < -1 && less(node->right->element, comparable))
            return rotate_left(node);

        // Left Right Case
        else if (balance > 1 && less(node->left->element, comparable)) {
            node->left = rotate_left(node->left);
            return rotate_right(node);
        }
        // Right Left Case
        else if (balance < -1 && less(comparable, node->right->element)) {
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
            node = node_allocator();
            if (node) {
                node->element = comparable;
                node->left = nullptr;
                node->right = nullptr;
                node->height = 1;
            }
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
    static node_t* insert(node_t* node, element_t&& element, node_allocator_at&& node_allocator) noexcept {
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

    /**
     * @brief Pops the root replacing it with one of descendants, if present.
     * @param comparable Any key comparable with stored elements.
     */
    static pop_result_t pop(node_t* node) noexcept {

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

    /**
     * @brief Searches for a matching ancestor and pops it out.
     * @param comparable Any key comparable with stored elements.
     */
    template <typename comparable_at>
    static pop_result_t pop(node_t* node, comparable_at&& comparable) noexcept {
        if (!node)
            return {node, {}};

        auto less = comparator_t {};
        if (less(comparable, node->element)) {
            auto downstream = pop(node->left, comparable);
            node->left = downstream.node;
            if (downstream.popped)
                node = rebalance_after_pop(node);
            return {node, std::move(downstream.popped)};
        }

        else if (less(node->element, comparable)) {
            auto downstream = pop(node->right, comparable);
            node->right = downstream.node;
            if (downstream.popped)
                node = rebalance_after_pop(node);
            return {node, std::move(downstream.popped)};
        }

        else
            // We have found the node to pop!
            return pop(node);
    }

    struct remove_if_result_t {
        node_t* root = nullptr;
        std::size_t count = 0;
    };

    template <typename predicate_at, typename node_deallocator_at>
    static remove_if_result_t remove_if(node_t* node,
                                        predicate_at&& predicate,
                                        node_deallocator_at&& node_deallocator) noexcept {
        return {};
    }

    static node_t* remove_range(node_t* node, node_range_t&& range) noexcept {
        return node;
    }
};

template <typename element_at,
          typename comparator_at,
          typename node_allocator_at = std::allocator<avl_node_gt<element_at, comparator_at>>>
class avl_tree_gt {
  public:
    using node_t = avl_node_gt<element_at, comparator_at>;
    using node_allocator_t = node_allocator_at;
    using element_t = element_at;

  private:
    node_t* root_ = nullptr;
    std::size_t size_ = 0;

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
        bool inserted = false;

        node_element_ref_t& operator=(element_t&& element) noexcept {
            node->element = element;
            return *this;
        }
    };

    template <typename comparable_at>
    node_element_ref_t upsert(comparable_at&& comparable) noexcept {
        auto result = node_t::find_or_make(root_, std::forward<comparable_at>(comparable), [] {
            return node_allocator_t {}.allocate(1);
        });
        root_ = result.root;
        return {result.match, result.inserted};
    }
};

/**
 * @brief Transactional Concurrent In-Memory Container with Snapshots support.
 *
 * @section Writes Consistency
 * Writing one entry or a batch is logically different.
 * Either all fail or all succeed. Thats why `set` and `set_many`
 * are implemented separately. Transactions write only on `submit`,
 * thus they don't need `set_many`.
 *
 * @section Read Consistency
 * Reading a batch of entries is same as reading one by one.
 * The received items might not be consistent with each other.
 * If such behaviour is needed - you must create snapshot.
 *
 * @section Pitfalls with WATCH-ing missing values
 * If an entry was missing. Then:
 *      1. WATCH-ed in a transaction.
 *      2. added in the second transaction.
 *      3. removed in the third transaction.
 * The first transaction will succeed, if we try to commit it.
 *
 */
template <typename key_at,
          typename value_at,
          typename key_comparator_at = std::less<>,
          typename key_hash_at = std::hash<key_at>,
          typename allocator_at = std::allocator<std::pair<key_at, value_at>>>
class acid_gt {
  public:
    using key_t = key_at;
    using value_t = value_at;
    using generation_t = std::size_t;
    using key_comparator_t = key_comparator_at;
    using key_hash_t = key_hash_at;
    using allocator_t = allocator_at;
    using allocator_traits_t = std::allocator_traits<allocator_t>;
    using acid_t = acid_gt;

    static_assert(std::is_trivially_copy_constructible<key_t>());
    static_assert(std::is_trivially_copy_assignable<key_t>());
    static_assert(std::is_nothrow_move_constructible<value_t>());
    static_assert(std::is_nothrow_move_assignable<value_t>());

    struct entry_t {
        value_t value;
        key_t key;
        generation_t generation = 0;
    };

    struct entry_generation_t {
        key_t key;
        generation_t generation = 0;
    };

    struct entry_comparator_t {
        bool operator()(key_t const& a, key_t const& b) const noexcept { return key_comparator_t {}(a, b); }
        bool operator()(entry_t const& a, entry_t const& b) const noexcept {
            auto is_less = key_comparator_t {}(a.key, b.key);
            auto is_more = key_comparator_t {}(b.key, a.key);
            return is_less || is_more ? is_less : a.generation < b.generation;
        }
    };

    using entries_alloc_t = typename allocator_traits_t::template rebind_alloc<entry_t>;
    using entries_tree_t = avl_tree_gt<entry_t, entry_comparator_t, entries_alloc_t>;

    using watched_alloc_t = typename allocator_traits_t::template rebind_alloc<entry_generation_t>;
    using watched_tree_t = avl_tree_gt<entry_generation_t, entry_comparator_t, watched_alloc_t>;

    using snapshots_alloc_t = typename allocator_traits_t::template rebind_alloc<generation_t>;
    using snapshots_tree_t = avl_tree_gt<generation_t, std::less<>, watched_alloc_t>;

    class snapshot_t {
        acid_t& acid_;
        generation_t generation_;
    };

    class transaction_t {
        acid_t& acid_;
        entries_tree_t updated_;
        watched_tree_t watched_;
        generation_t generation_;

      public:
        bool watch(key_t const&) noexcept;
        bool contains(key_t const&, bool watch = true) noexcept;
        bool get(key_t const&, value_t& value, bool watch = true) noexcept;
        bool set(key_t const&, value_t const& value, bool watch = true) noexcept;
        bool next(key_t const&, key_t&, bool watch = true) noexcept;

        bool reset() noexcept;
        bool commit() noexcept;

        template <typename callback_found_at, typename callback_missing_at>
        void for_one(key_t const& key,
                     callback_found_at&& callback_found,
                     callback_missing_at& callback_missing,
                     bool watch = false) noexcept {}
    };

  private:
    entries_tree_t entries_;
    snapshots_tree_t snapshots_;

  public:
    snapshot_t snapshot();
    transaction_t transaction(bool snapshot = false);

    bool set(key_t const& key, value_t&& value) noexcept {
        auto ref = entries_.upsert(key);
        if (!ref)
            return false;
        ref = value;
        return true;
    }

    template <typename keys_iterator_at, typename values_iterator_at>
    bool set_many(keys_iterator_at keys_begin, keys_iterator_at keys_end, values_iterator_at values) noexcept {
        // - find generation Y of last needed snapshot
        // - choose generation X newer than every running transaction
        // - reserve N nodes in allocator
        // - pre-construct those nodes and move values into them
        // - for every key:
        //      - lock parent tree
        //      - insert key+X
        //      - move-assign the value
        // - if a failure occurs on any one of the inserts, for each key:
        //      - lock parent tree
        //      - remove the key+X
        // - if all succeeded, for each key:
        //      - if it was a removal operation:
        //          - if key+Y or earlier entries with that key exist:
        //              -
        //      - else:
        //          -
    }

    bool contains(key_t const& key) noexcept { return entries_.find(key) != nullptr; }

    bool next(key_t const& key, key_t& result) noexcept {
        auto it = entries_.find_successor(key);
        if (!it)
            return false;
        result = it->element.key;
        return true;
    }

    bool remove_range(key_t const&, key_t const&) noexcept;

    template <typename callback_at>
    void for_range(key_t const&, key_t const&, callback_at&&) noexcept;

    template <typename callback_found_at, typename callback_missing_at>
    void for_one(key_t const& key, callback_found_at&& callback_found, callback_missing_at& callback_missing) noexcept {
        auto it = entries_.find(key);
        return it ? callback_found(it->element.value) : callback_missing(it->element.value);
    }
};

} // namespace unum::ukv
