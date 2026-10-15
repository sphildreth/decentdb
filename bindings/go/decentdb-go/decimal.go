package decentdb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

var (
	_ sql.Scanner   = (*Decimal)(nil)
	_ driver.Valuer = Decimal{}
)

func (d Decimal) validate() error {
	if d.Scale < 0 || d.Scale > 255 {
		return fmt.Errorf("decentdb: decimal scale must be between 0 and 255, got %d", d.Scale)
	}
	return nil
}

// String returns the exact base-ten value, retaining the stored scale.
func (d Decimal) String() string {
	if err := d.validate(); err != nil {
		return "<invalid decimal>"
	}
	digits := strconv.FormatInt(d.Unscaled, 10)
	if d.Scale == 0 {
		return digits
	}
	sign := ""
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}
	if len(digits) <= d.Scale {
		digits = strings.Repeat("0", d.Scale+1-len(digits)) + digits
	}
	point := len(digits) - d.Scale
	return sign + digits[:point] + "." + digits[point:]
}

// Value implements database/sql/driver.Valuer with an exact decimal string.
// The driver binds Decimal values natively; ORM formatters can use this string
// without converting through float64. Column context supplies NUMERIC typing.
func (d Decimal) Value() (driver.Value, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	return d.String(), nil
}

// Scan implements database/sql.Scanner for exact native or textual decimals.
// SQL NULL requires a nullable *Decimal destination; float64 is rejected because
// conversion cannot recover decimal precision already lost to binary floating point.
func (d *Decimal) Scan(src any) error {
	if d == nil {
		return fmt.Errorf("decentdb: cannot scan into a nil Decimal")
	}
	var next Decimal
	var err error
	switch value := src.(type) {
	case Decimal:
		next = value
	case int64:
		next = Decimal{Unscaled: value}
	case string:
		next, err = parseDecimalText(value)
	case []byte:
		next, err = parseDecimalText(string(value))
	default:
		err = fmt.Errorf("decentdb: cannot scan %T into Decimal", src)
	}
	if err == nil {
		err = next.validate()
	}
	if err != nil {
		return err
	}
	*d = next
	return nil
}

func parseDecimalText(text string) (Decimal, error) {
	invalid := func() (Decimal, error) {
		return Decimal{}, fmt.Errorf("decentdb: invalid or out-of-range decimal %q", text)
	}
	mantissa := strings.TrimSpace(text)
	exponent := 0
	if index := strings.IndexAny(mantissa, "eE"); index >= 0 {
		var err error
		exponent, err = strconv.Atoi(mantissa[index+1:])
		if err != nil || exponent < -255 || exponent > 255 {
			return invalid()
		}
		mantissa = mantissa[:index]
	}
	sign := ""
	if strings.HasPrefix(mantissa, "-") || strings.HasPrefix(mantissa, "+") {
		sign, mantissa = mantissa[:1], mantissa[1:]
	}
	point := strings.IndexByte(mantissa, '.')
	scale := -exponent
	if point >= 0 {
		scale += len(mantissa) - point - 1
		mantissa = mantissa[:point] + mantissa[point+1:]
	}
	if mantissa == "" {
		return invalid()
	}
	for _, digit := range mantissa {
		if digit < '0' || digit > '9' {
			return invalid()
		}
	}
	coefficient, ok := new(big.Int).SetString(sign+mantissa, 10)
	if !ok {
		return invalid()
	}
	if scale < 0 {
		coefficient.Mul(coefficient, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil))
		scale = 0
	}
	// Normalize only when needed to fit the native coefficient/scale limits.
	for scale > 0 && (scale > 255 || !coefficient.IsInt64()) {
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(coefficient, big.NewInt(10), remainder)
		if remainder.Sign() != 0 {
			return invalid()
		}
		coefficient = quotient
		scale--
	}
	if scale > 255 || !coefficient.IsInt64() {
		return invalid()
	}
	return Decimal{Unscaled: coefficient.Int64(), Scale: scale}, nil
}
