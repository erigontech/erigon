package util

import (
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestMultiplyByElement(t *testing.T) {
	assert := assert.New(t)

	vec1 := []float64{2.0, 2.0, 3.0}
	vec2 := []float64{2.5, 6.0, 4.0}

	result, err := Vector.MultiplyByElement(vec1, vec2)
	assert.Nil(err)
	assert.Equal([]float64{5.0, 12.0, 12.0}, result)

	vec1 = []float64{2.0, 3.0}
	vec2 = []float64{2.5, 6.0, 4.0}

	result, err = Vector.MultiplyByElement(vec1, vec2)
	assert.NotNil(err)
	assert.Nil(result)
}

func TestNormalize(t *testing.T) {
	assert := assert.New(t)

	vec := []float64{3.0, 4.0}
	result, err := Vector.Normalize(vec)
	assert.Equal([]float64{0.6, 0.8}, result)

	vec = []float64{0, 0, 0, 0}
	result, err = Vector.Normalize(vec)
	assert.NotNil(err)
	assert.Equal([]float64{0, 0, 0, 0}, result)
}

func TestGetMagnitude(t *testing.T) {
	assert := assert.New(t)

	vec := []float64{3.0, 4.0}
	result := Vector.GetMagnitude(vec)
	assert.Equal(5.0, result)

	vec = []float64{1.0, 1.0, 1.0, 1.0}
	result = Vector.GetMagnitude(vec)
	assert.Equal(2.0, result)
}
