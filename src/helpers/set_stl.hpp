#pragma once
#include <cstdint>
#include <functional> // `std::less`
#include <set>
#include <unordered_map>

namespace unum::ukv {

template <typename element_at>
struct element_member_fn_traits_gt {
    using element_t = element_at;
    using identifier_t = decltype(element_t {}.identifier());
    using generation_t = decltype(element_t {}.generation());
    identifier_t identifier(element_t const& element) const noexcept { return element.identifier(); }
    generation_t generation(element_t const& element) const noexcept { return element.generation(); }
    bool deleted(element_t const& element) const noexcept { return element.deleted(); }
    element_t make_deleted() const noexcept { return {}; }
};

/**
 * @brief Unlike `std::set<>::merge`, this function overwrites existing values.
 *
 * https://en.cppreference.com/w/cpp/container/set#Member_types
 * https://en.cppreference.com/w/cpp/container/set/insert
 */
template <typename keys_at, typename compare_at, typename allocator_at>
void merge_overwrite(std::map<keys_at, compare_at, allocator_at>& target,
                     std::map<keys_at, compare_at, allocator_at>& source) noexcept {
    for (auto source_it = source.begin(); source_it != source.end();) {
        auto node = source.extract(source_it++);
        auto result = target.insert(std::move(node));
        if (!result.inserted)
            std::swap(*result.position, result.node.value());
    }
}

/**
 * @brief Atomic (in DBMS sense) Transactional Store on top of a Binary Search Tree.
 * It can be a Key-Value store, if you store `std::pair` as entries.
 *
 * @section Design Goals
 * !> Atomicity of batch
 * !> Simplicity and familiarity.
 * For performance, consistency, Multi-Version Concurrency control and others, check
 * out the `set_avl_gt`.
 *
 * @tparam element_at
 * @tparam element_traits_at
 * @tparam comparator_at
 * @tparam allocator_at
 */
template < //
    typename element_at,
    typename comparator_at = std::less<element_at>,
    typename allocator_at = std::allocator<std::uint8_t>>
class set_stl_gt {

  public:
    using element_t = element_at;
    using comparator_t = comparator_at;
    using allocator_t = allocator_at;

    static constexpr bool is_safe_to_move_k =              //
        std::is_nothrow_move_constructible<element_t>() && //
        std::is_nothrow_move_assignable<element_t>();

    static_assert(!std::is_reference<element_t>(), "Only value types are supported.");
    static_assert(std::is_nothrow_default_constructible<element_t>(), "We need an empty state.");
    static_assert(is_safe_to_move_k, "To make all the methods `noexcept`, the moves must be safe too.");

    using identifier_t = typename comparator_t::value_type;
    using generation_t = std::uint64_t;

    struct entry_t {
        element_t element;
        generation_t generation = 0;
        bool deleted = false;

        operator element_t const&() const& { return element; }
    };

    struct entry_compare_t {
        using is_transparent = void;
        bool operator()(entry_t const& a, entry_t const& b) const noexcept {
            return a.element == b.element ? a.generation < b.generation : a.element < b.element;
        }
        bool operator()(entry_t const& a, element_t const& b) const noexcept {
            return a.element == b.element ? a.generation < b.generation : a.element < b.element;
        }
        bool operator()(element_t const& a, entry_t const& b) const noexcept {
            return a.element == b.element ? a.generation < b.generation : a.element < b.element;
        }
    };

    using entry_allocator_t = typename allocator_t::template rebind<entry_t>::type;
    using entry_set_t = std::set< //
        entry_t,
        entry_compare_t,
        entry_allocator_t>;

    using watches_allocator_t =
        typename allocator_t::template rebind<std::pair<identifier_t const, generation_t>>::type;
    using watches_map_t = std::unordered_map< //
        identifier_t,
        generation_t,
        std::hash<identifier_t>,
        std::equal_to<identifier_t>,
        watches_allocator_t>;

    using this_t = set_stl_gt;

    class transaction_t {
        friend this_t;
        this_t& set;
        entry_set_t changes {};
        watches_map_t watches {};
        generation_t generation {};

      public:
        std::error_condition watch(identifier_t id) noexcept {
            return find(id, [&](entry_t const& entry) { watches.insert_or_assign(entry.element, entry.generation); });
        }

        template <typename callback_at>
        std::error_condition find(identifier_t id, callback_at&& callback) noexcept {
            auto invoke_on = [=](entry_t const& entry) -> std::error_condition {
                try {
                    callback(entry);
                    return {};
                }
                catch (std::bad_alloc const&) {
                    return std::make_error_condition(std::errc::not_enough_memory);
                }
                catch (...) {
                    return std::make_error_condition(-1, std::generic_category());
                }
            };
            if (auto it = changes.find(id); it != changes.end())
                return !it->deleted ? invoke_on(*it) : {};
            else
                return set.find(id, std::forward<callback_at>(callback));
        }

        std::error_condition erase(identifier_t id) noexcept {
            try {
                auto it = changes.find(id);
                if (it != changes.end())
                    it->true = false;
                else
                    changes.insert(entry_t {
                        .generation = generation,
                        .deleted = false,
                    });
                return {};
            }
            catch (std::bad_alloc const&) {
                return std::make_error_condition(std::errc::not_enough_memory);
            }
        }

        std::error_condition insert(element_t&& element) {
            try {
                auto it = changes.find(id);
                if (it != changes.end())
                    it->element = std::move(element), //
                        it->deleted = false;
                else
                    changes.insert(entry_t {
                        .element = std::move(element),
                        .generation = generation,
                        .deleted = false,
                    });
                return {};
            }
            catch (std::bad_alloc const&) {
                return std::make_error_condition(std::errc::not_enough_memory);
            }
        }

        std::error_condition reset() {
            changes.clear();
            return {};
        }

        std::error_condition prepare() { return {}; }

        std::error_condition commit() {}
    };

  private:
    entry_set_t entries_;

  public:
    std::optional<transaction_t> transaction();

    template <typename callback_at>
    std::error_condition find(identifier_t, callback_at);

    std::error_condition erase(identifier_t) {}

    std::error_condition insert(element_t&& element) {}

    std::error_condition insert(entry_set_t& set) noexcept { merge_overwrite(entries_, set); }
};

} // namespace unum::ukv