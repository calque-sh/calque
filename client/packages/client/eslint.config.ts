import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  {
    ignores: ["dist", "coverage", "target"],
  },
  eslint.configs.recommended,
  ...tseslint.configs.strict,
  {
    files: ["**/*.ts"],
    languageOptions: {
      parserOptions: { tsconfigRootDir: import.meta.dirname },
    },
  },
);
