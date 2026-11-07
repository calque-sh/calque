import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist", "coverage"] },
  eslint.configs.recommended,
  ...tseslint.configs.strict,
  {
    files: ["**/*.ts", "**/*.tsx"],
    languageOptions: {
      parserOptions: { tsconfigRootDir: import.meta.dirname },
    },
  },
);
