package rpc

import (
	"encoding/json"
	"testing"

	jsoniter "github.com/json-iterator/go"
)

func BenchmarkJsonMarshal(b *testing.B) {
	var data = []byte(`[
  {
    "Contient": "Africa",
    "Country_State": "NaN",
    "cuisine": "Missouri",
    "title": "Ground Beef and Cabbage",
    "URL": "https://www.allrecipes.com/recipe/229324/ground-beef-and-cabbage/",
    "rating": 4.5,
    "total_time": 60,
    "prep_time": 15,
    "cook_time": 45,
    "description": "This ground beef and cabbage recipe combines lean ground beef, cabbage, onion, and tomatoes with simple seasonings for a hearty and comforting dish.",
    "ingredients": [
      "1 large head cabbage, finely chopped",
      "1 (14.5 ounce) can diced tomatoes with juice",
      "1 onion, halved and thinly sliced",
      "1 tablespoon Italian seasoning",
      "salt and ground black pepper to taste",
      "1 pound lean ground beef"
    ],
    "instructions": [
      "Place cabbage, tomatoes with juice, onion, Italian seasoning, salt, and pepper into a Dutch oven or large pot over low heat; cook and stir until it begins to simmer.",
      "Add lean ground beef on top; cover and cook, stirring occasionally, until cabbage is tender and ground beef is cooked through, about 45 minutes."
    ],
    "nutrients": {
      "calories": "228 kcal",
      "carbohydrateContent": "18 g",
      "cholesterolContent": "50 mg",
      "fiberContent": "7 g",
      "proteinContent": "18 g",
      "saturatedFatContent": "4 g",
      "sodiumContent": "191 mg",
      "sugarContent": "10 g",
      "fatContent": "10 g",
      "unsaturatedFatContent": "0 g"
    },
    "serves": "6 servings"
  },
  {
    "Contient": "Africa",
    "Country_State": "NaN",
    "cuisine": "Missouri",
    "title": "Old Fashioned Peach Cobbler",
    "URL": "https://www.allrecipes.com/recipe/19897/old-fashioned-peach-cobbler/",
    "rating": 4.6,
    "total_time": 130,
    "prep_time": 30,
    "cook_time": 70,
    "description": "This old-fashioned peach cobbler recipe features flaky, homemade pastry filled with fresh peaches tossed in citrus juice with nutmeg and cinnamon.",
    "ingredients": [
      "2.5 cups all-purpose flour",
      "4 tablespoons white sugar, divided",
      "1 teaspoon salt",
      "1 cup shortening",
      "1 large egg",
      "0.25 cup cold water",
      "1 tablespoon butter, melted",
      "3 pounds fresh peaches - peeled, pitted, and sliced",
      "0.75 cup orange juice",
      "0.25 cup lemon juice",
      "0.5 cup butter",
      "2 cups white sugar",
      "1 tablespoon cornstarch",
      "1 teaspoon ground cinnamon",
      "0.5 teaspoon ground nutmeg"
    ],
    "instructions": [
      "Make crust: Sift together flour, 3 tablespoons sugar, and salt in a medium bowl. Work in shortening with a pastry blender until mixture resembles coarse crumbs. Whisk together egg and cold water in a small bowl. Sprinkle over flour mixture; work with hands to form dough into a ball. Wrap with plastic wrap and chill in the refrigerator for 30 minutes.",
      "Preheat the oven to 350 degrees F (175 degrees C).",
      "Roll out 1/2 of the chilled dough to 1/8-inch thickness. Place in a 9x13-inch baking dish, covering the bottom and halfway up the sides.",
      "Bake in the preheated oven until golden brown, about 20 minutes.",
      "Make filling: Mix peaches, orange juice, and lemon juice in a large saucepan. Add butter and cook over medium-low heat until butter is melted. Stir together sugar, cornstarch, cinnamon, and nutmeg in a bowl; mix into peach mixture until combined. Pour into baked crust.",
      "Roll remaining dough to 1/4-inch thickness. Cut into 1/2-inch-wide strips. Weave strips into a lattice over peaches. Sprinkle with 1 tablespoon sugar, then drizzle with melted butter.",
      "Bake in the preheated oven until top crust is golden brown, 35 to 40 minutes."
    ],
    "nutrients": {
      "calories": "338 kcal",
      "carbohydrateContent": "44 g",
      "cholesterolContent": "26 mg",
      "fiberContent": "1 g",
      "proteinContent": "2 g",
      "saturatedFatContent": "7 g",
      "sodiumContent": "177 mg",
      "sugarContent": "30 g",
      "fatContent": "18 g",
      "unsaturatedFatContent": "0 g"
    },
    "serves": "18 servings"
  },
{
  "Contient": "Africa",
  "Country_State": "NaN",
  "cuisine": "Missouri",
  "title": "Amish Friendship Bread Starter",
  "URL": "https://www.allrecipes.com/recipe/7063/amish-friendship-bread-starter/",
  "rating": 4.7,
  "total_time": 14440,
  "prep_time": 30,
  "cook_time": 10,
  "description": "Amish friendship bread starter is made with yeast, sugar, milk, and flour fermented to create a sweet sourdough starter for sharing with friends.",
  "ingredients": [
    "1 (.25 ounce) package active dry yeast",
    "0.25 cup warm water (110 degrees F/45 degrees C)",
    "3 cups all-purpose flour, divided",
    "3 cups white sugar, divided",
    "3 cups milk, divided"
  ],
  "instructions": [
    "Dissolve yeast in warm water in a small bowl; let stand until foamy, about 10 minutes.",
    "Combine 1 cup flour and 1 cup sugar in a 2-quart container (glass, plastic, or ceramic); mix thoroughly. Stir in 1 cup milk and yeast mixture. Cover the container loosely and leave at room temperature until bubbly. This is day 1 of the 10-day process.",
    "Days 2 through 4: Stir starter with a spoon. Day 5: Stir in 1 cup flour, 1 cup sugar, and 1 cup milk. Days 6 through 9: Stir starter with a spoon.",
    "Day 10: Stir in remaining 1 cup flour, 1 cup sugar, and 1 cup milk. Remove 1 cup starter to make your first bread. Give 2 cups starter to friends (1 cup each) along with this recipe and your favorite Amish bread recipe. Store remaining 1 cup starter in a container in the refrigerator or begin the 10-day process over again (beginning with step 2)."
  ],
  "nutrients": {
    "calories": "34 kcal",
    "carbohydrateContent": "8 g",
    "cholesterolContent": "1 mg",
    "fiberContent": "0 g",
    "proteinContent": "1 g",
    "saturatedFatContent": "0 g",
    "sodiumContent": "3 mg",
    "sugarContent": "5 g",
    "fatContent": "0 g",
    "unsaturatedFatContent": "0 g"
  },
  "serves": "120 servings"
}]`)

	type Nutrients struct {
		Calories              string `json:"calories"`
		CarbohydrateContent   string `json:"carbohydrateContent"`
		CholesterolContent    string `json:"cholesterolContent"`
		FiberContent          string `json:"fiberContent"`
		ProteinContent        string `json:"proteinContent"`
		SaturatedFatContent   string `json:"saturatedFatContent"`
		SodiumContent         string `json:"sodiumContent"`
		SugarContent          string `json:"sugarContent"`
		FatContent            string `json:"fatContent"`
		UnsaturatedFatContent string `json:"unsaturatedFatContent"`
	}

	type Recipe struct {
		Continent    string    `json:"Contient"`
		CountryState string    `json:"Country_State"`
		Cuisine      string    `json:"cuisine"`
		Title        string    `json:"title"`
		URL          string    `json:"URL"`
		Rating       float64   `json:"rating"`
		TotalTime    int       `json:"total_time"`
		PrepTime     int       `json:"prep_time"`
		CookTime     int       `json:"cook_time"`
		Description  string    `json:"description"`
		Ingredients  []string  `json:"ingredients"`
		Instructions []string  `json:"instructions"`
		Nutrients    Nutrients `json:"nutrients"`
		Serves       string    `json:"serves"`
	}

	b.Run("std-unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		var recipes []Recipe
		for i := 0; i < b.N; i++ {
			if err := json.Unmarshal(data, &recipes); err != nil {
				b.Fatalf("Failed to unmarshal using standard lib. %v", err)
			}
		}
	})

	var recipes []Recipe
	b.Run("jsoniter-unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		for i := 0; i < b.N; i++ {
			if err := json.Unmarshal(data, &recipes); err != nil {
				b.Fatalf("Failed to unmarshal using standard lib. %v", err)
			}
		}
	})

	b.Run("std-marshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(recipes); err != nil {
				b.Fatalf("Failed to marshal using standard lib. %v", err)
			}
		}
	})

	b.Run("jsoniter-marshal", func(b *testing.B) {
		b.ReportAllocs()
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		for i := 0; i < b.N; i++ {
			if _, err := json.Marshal(recipes); err != nil {
				b.Fatalf("Failed to marshal using standard lib. %v", err)
			}
		}
	})
}
