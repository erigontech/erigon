#include "fraction_field_element.h"

#include "error_handling.h"

namespace starkware {

template <typename FieldElementT>
FractionFieldElement<FieldElementT> FractionFieldElement<FieldElementT>::operator+(
    const FractionFieldElement<FieldElementT>& rhs) const {
  const auto num_value = this->numerator_ * rhs.denominator_ + this->denominator_ * rhs.numerator_;
  const auto denom_value = this->denominator_ * rhs.denominator_;
  return FractionFieldElement(num_value, denom_value);
}

template <typename FieldElementT>
FractionFieldElement<FieldElementT> FractionFieldElement<FieldElementT>::operator-(
    const FractionFieldElement<FieldElementT>& rhs) const {
  const auto num_value = this->numerator_ * rhs.denominator_ - this->denominator_ * rhs.numerator_;
  const auto denom_value = this->denominator_ * rhs.denominator_;
  return FractionFieldElement(num_value, denom_value);
}

template <typename FieldElementT>
FractionFieldElement<FieldElementT> FractionFieldElement<FieldElementT>::operator*(
    const FractionFieldElement<FieldElementT>& rhs) const {
  return FractionFieldElement(
      this->numerator_ * rhs.numerator_, this->denominator_ * rhs.denominator_);
}

template <typename FieldElementT>
bool FractionFieldElement<FieldElementT>::operator==(
    const FractionFieldElement<FieldElementT>& rhs) const {
  return this->numerator_ * rhs.denominator_ == this->denominator_ * rhs.numerator_;
}

template <typename FieldElementT>
FractionFieldElement<FieldElementT> FractionFieldElement<FieldElementT>::Inverse() const {
  ASSERT(this->numerator_ != FieldElementT::Zero(), "Zero does not have an inverse");
  return FractionFieldElement(denominator_, numerator_);
}

}  // namespace starkware
