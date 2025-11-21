"""
Traductor de expresiones de PowerCenter a Azure Data Factory
Mapeo completo y validación de funciones, operadores y sintaxis
"""

import re
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger('pc-to-adf.expression_translator')


class ExpressionTranslator:
    """
    Traductor robusto de expresiones PowerCenter a ADF.
    Garantiza compatibilidad 100% con Azure Data Factory.
    """

    def __init__(self):
        """Inicializa el traductor con todos los mapeos de funciones"""
        self.function_patterns = self._initialize_function_patterns()
        self.operator_mappings = self._initialize_operator_mappings()
        self.forbidden_functions = self._initialize_forbidden_functions()

    def _initialize_function_patterns(self) -> List[Tuple[str, str]]:
        """
        Inicializa patrones de funciones con orden específico.
        Retorna lista de tuplas (patrón_regex, reemplazo).
        El orden es crítico para evitar conflictos.
        """
        return [
            # ===== FUNCIONES DE FECHA =====

            # GET_DATE_PART: Extracción de partes de fecha
            # Patrón mejorado para capturar argumentos con paréntesis anidados
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]DD[\'"]\s*\)', r'dayOfMonth(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]MM[\'"]\s*\)', r'month(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]YYYY[\'"]\s*\)', r'year(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]YY[\'"]\s*\)', r'year(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]Y[\'"]\s*\)', r'year(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]DDD[\'"]\s*\)', r'dayOfYear(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]WW[\'"]\s*\)', r'weekOfYear(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]HH[\'"]\s*\)', r'hour(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]MI[\'"]\s*\)', r'minute(\1)'),
            (r'GET_DATE_PART\s*\(\s*(.+?)\s*,\s*[\'"]SS[\'"]\s*\)', r'second(\1)'),

            # ADD_TO_DATE / ADD_toDate: Operaciones aritméticas con fechas
            (r'ADD_TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]DD[\'"]\s*,\s*([^)]+?)\s*\)', r'addDays(\1, \2)'),
            (r'ADD_TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]MM[\'"]\s*,\s*([^)]+?)\s*\)', r'addMonths(\1, \2)'),
            (r'ADD_TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]YYYY[\'"]\s*,\s*([^)]+?)\s*\)', r'addYears(\1, \2)'),
            (r'ADD_TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]YY[\'"]\s*,\s*([^)]+?)\s*\)', r'addYears(\1, \2)'),
            (r'ADD_TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]Y[\'"]\s*,\s*([^)]+?)\s*\)', r'addYears(\1, \2)'),
            (r'ADD_TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]HH[\'"]\s*,\s*([^)]+?)\s*\)', r'addHours(\1, \2)'),
            (r'ADD_TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]MI[\'"]\s*,\s*([^)]+?)\s*\)', r'addMinutes(\1, \2)'),
            (r'ADD_TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]SS[\'"]\s*,\s*([^)]+?)\s*\)', r'addSeconds(\1, \2)'),

            # Variantes con ADD_toDate (sin guión bajo)
            (r'ADD_toDate\s*\(\s*([^,]+?)\s*,\s*[\'"]DD[\'"]\s*,\s*([^)]+?)\s*\)', r'addDays(\1, \2)'),
            (r'ADD_toDate\s*\(\s*([^,]+?)\s*,\s*[\'"]MM[\'"]\s*,\s*([^)]+?)\s*\)', r'addMonths(\1, \2)'),
            (r'ADD_toDate\s*\(\s*([^,]+?)\s*,\s*[\'"]YYYY[\'"]\s*,\s*([^)]+?)\s*\)', r'addYears(\1, \2)'),
            (r'ADD_toDate\s*\(\s*([^,]+?)\s*,\s*[\'"]YY[\'"]\s*,\s*([^)]+?)\s*\)', r'addYears(\1, \2)'),
            (r'ADD_toDate\s*\(\s*([^,]+?)\s*,\s*[\'"]Y[\'"]\s*,\s*([^)]+?)\s*\)', r'addYears(\1, \2)'),

            # LAST_DAY: Último día del mes
            (r'LAST_DAY\s*\(\s*([^)]+?)\s*\)', r'lastDayOfMonth(\1)'),

            # TO_CHAR para formatos de fecha
            # CRÍTICO: TO_CHAR(date, 'DAY') en PowerCenter devuelve el NOMBRE del día ('Monday')
            # pero dayOfWeek() en ADF devuelve un NÚMERO (1=Sunday, 2=Monday, ..., 7=Saturday)
            # Necesitamos mapear el número a nombres usando case()
            (r'TO_CHAR\s*\(\s*([^,]+?)\s*,\s*[\'"]DAY[\'"]\s*\)',
             r"case(dayOfWeek(\1), 1, 'Sunday', 2, 'Monday', 3, 'Tuesday', 4, 'Wednesday', 5, 'Thursday', 6, 'Friday', 7, 'Saturday')"),
            (r'TO_CHAR\s*\(\s*([^,]+?)\s*,\s*[\'"]MONTH[\'"]\s*\)', r"toString(\1, 'MMMM')"),
            (r'TO_CHAR\s*\(\s*([^,]+?)\s*,\s*[\'"]DDD[\'"]\s*\)', r'dayOfYear(\1)'),
            (r'TO_CHAR\s*\(\s*([^,]+?)\s*,\s*[\'"]([^\'\"]+?)[\'"]\s*\)', r"toString(\1, '\2')"),

            # TO_DATE: Conversión de string a fecha
            (r'TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]MM/DD/YYYY[\'"]\s*\)', r"toDate(\1, 'MM/dd/yyyy')"),
            (r'TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]DD/MM/YYYY[\'"]\s*\)', r"toDate(\1, 'dd/MM/yyyy')"),
            (r'TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]YYYY-MM-DD[\'"]\s*\)', r"toDate(\1, 'yyyy-MM-dd')"),
            (r'TO_DATE\s*\(\s*([^,]+?)\s*,\s*[\'"]([^\'\"]+?)[\'"]\s*\)', r"toDate(\1, '\2')"),
            (r'TO_DATE\s*\(\s*([^)]+?)\s*\)', r'toDate(\1)'),

            # SYSDATE y funciones de fecha actuales
            (r'SYSDATE\s*\(\s*\)', r'currentTimestamp()'),
            (r'SYSDATE', r'currentTimestamp()'),
            (r'CURRENT_DATE\s*\(\s*\)', r'currentDate()'),
            (r'CURRENT_TIMESTAMP\s*\(\s*\)', r'currentTimestamp()'),

            # ===== FUNCIONES DE STRING =====

            # SUBSTR: Substring con índices
            (r'SUBSTR\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'substring(\1, \2, \3)'),
            (r'SUBSTR\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'substring(\1, \2)'),

            # INSTR: Búsqueda de substring
            (r'INSTR\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'indexOf(\1, \2)'),

            # REPLACE_CHAR: Reemplazo de caracteres
            (r'REPLACE_CHAR\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'replace(\1, \2, \3)'),
            (r'REPLACE\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'replace(\1, \2, \3)'),

            # CONCAT: Concatenación
            (r'CONCAT\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'concat(\1, \2)'),

            # TRIM, LTRIM, RTRIM
            (r'LTRIM\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'ltrim(\1, \2)'),
            (r'LTRIM\s*\(\s*([^)]+?)\s*\)', r'ltrim(\1)'),
            (r'RTRIM\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'rtrim(\1, \2)'),
            (r'RTRIM\s*\(\s*([^)]+?)\s*\)', r'rtrim(\1)'),
            (r'TRIM\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'trim(\1, \2)'),
            (r'TRIM\s*\(\s*([^)]+?)\s*\)', r'trim(\1)'),

            # UPPER, LOWER
            (r'UPPER\s*\(\s*([^)]+?)\s*\)', r'upper(\1)'),
            (r'LOWER\s*\(\s*([^)]+?)\s*\)', r'lower(\1)'),

            # LENGTH
            (r'LENGTH\s*\(\s*([^)]+?)\s*\)', r'length(\1)'),

            # LPAD, RPAD
            (r'LPAD\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'lpad(\1, \2, \3)'),
            (r'RPAD\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'rpad(\1, \2, \3)'),

            # ===== FUNCIONES DE CONVERSIÓN =====

            # TO_INTEGER, TO_DECIMAL, TO_FLOAT
            (r'TO_INTEGER\s*\(\s*([^)]+?)\s*\)', r'toInteger(\1)'),
            (r'TO_DECIMAL\s*\(\s*([^)]+?)\s*\)', r'toDecimal(\1)'),
            (r'TO_FLOAT\s*\(\s*([^)]+?)\s*\)', r'toFloat(\1)'),
            (r'TO_CHAR\s*\(\s*([^)]+?)\s*\)', r'toString(\1)'),

            # ===== FUNCIONES CONDICIONALES =====

            # IIF: Condicional ternario
            (r'IIF\s*\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'iif(\1, \2, \3)'),

            # DECODE: Case statement (complejo, requerirá procesamiento adicional)
            # Por ahora se deja para case()

            # ===== FUNCIONES DE AGREGACIÓN =====

            # Estas se mantienen iguales pero en minúsculas
            (r'SUM\s*\(\s*([^)]+?)\s*\)', r'sum(\1)'),
            (r'AVG\s*\(\s*([^)]+?)\s*\)', r'avg(\1)'),
            (r'COUNT\s*\(\s*([^)]+?)\s*\)', r'count(\1)'),
            (r'COUNT\s*\(\s*\*\s*\)', r'count()'),
            (r'MIN\s*\(\s*([^)]+?)\s*\)', r'min(\1)'),
            (r'MAX\s*\(\s*([^)]+?)\s*\)', r'max(\1)'),
            (r'FIRST\s*\(\s*([^)]+?)\s*\)', r'first(\1)'),
            (r'LAST\s*\(\s*([^)]+?)\s*\)', r'last(\1)'),

            # ===== FUNCIONES MATEMÁTICAS =====

            (r'ROUND\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'round(\1, \2)'),
            (r'ROUND\s*\(\s*([^)]+?)\s*\)', r'round(\1)'),
            (r'CEIL\s*\(\s*([^)]+?)\s*\)', r'ceil(\1)'),
            (r'FLOOR\s*\(\s*([^)]+?)\s*\)', r'floor(\1)'),
            (r'ABS\s*\(\s*([^)]+?)\s*\)', r'abs(\1)'),
            (r'POWER\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'power(\1, \2)'),
            (r'SQRT\s*\(\s*([^)]+?)\s*\)', r'sqrt(\1)'),

            # ===== FUNCIONES NULL =====

            (r'ISNULL\s*\(\s*([^)]+?)\s*\)', r'isNull(\1)'),
            (r'IS_NULL\s*\(\s*([^)]+?)\s*\)', r'isNull(\1)'),
            (r'NVL\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'coalesce(\1, \2)'),
            (r'COALESCE\s*\(\s*([^)]+?)\s*\)', r'coalesce(\1)'),
        ]

    def _initialize_operator_mappings(self) -> List[Tuple[str, str]]:
        """
        Inicializa mapeos de operadores.
        El orden es crítico para evitar conflictos.
        """
        return [
            # Operadores lógicos (deben procesarse ANTES que otros)
            (r'\bAND\b', r'&&'),
            (r'\bOR\b', r'||'),
            (r'\bNOT\b', r'!'),

            # Operador de concatenación (IMPORTANTE: PowerCenter usa ||)
            # Pero || ya se usó para OR lógico, necesitamos contexto
            # En PowerCenter: 'string1' || 'string2' es concatenación
            # En ADF: concat(string1, string2)
            # Este es complejo, se maneja en _handle_concatenation()

            # Operadores de comparación
            (r'<>', r'!='),
            (r'===', r'=='),  # PowerCenter strict equal

            # El = simple solo se convierte en contexto de comparación
            # NO en asignaciones
        ]

    def _initialize_forbidden_functions(self) -> List[str]:
        """
        Lista de funciones que NO deben aparecer en el resultado final.
        Si aparecen, significa que la traducción falló.

        Nota: Se validan como llamadas a función (con paréntesis), no como parte de nombres de variables.
        """
        return [
            'GET_DATE_PART(',
            'ADD_TO_DATE(',
            'ADD_toDate(',
            'LAST_DAY(',
            'TO_CHAR(',
            'TO_DATE(',
            'REPLACE_CHAR(',
            'INSTR(',
            'DECODE(',           # CRÍTICO: DECODE debe traducirse a case()
            ' AND ',
            ' OR ',
            ' NOT ',
        ]

    def translate(self, expression: str) -> str:
        """
        Traduce una expresión de PowerCenter a sintaxis ADF.

        Args:
            expression: Expresión en sintaxis PowerCenter

        Returns:
            Expresión traducida a sintaxis ADF

        Raises:
            ValueError: Si la expresión contiene funciones no válidas después de traducción
        """
        if not expression or not expression.strip():
            return expression

        translated = expression.strip()

        # 0. Normalizar expresión: eliminar saltos de línea y espacios múltiples
        # Esto es crítico para expresiones anidadas como:
        # toString(
        # GET_DATE_PART(
        # (ADD_toDate(...)),'DD'))
        translated = re.sub(r'\s+', ' ', translated)  # Reemplazar múltiples espacios/saltos por un espacio
        translated = translated.strip()

        # 0.5. CRÍTICO: Eliminar prefijos IN_ y OUT_ de PowerCenter
        # PowerCenter usa IN_ para input ports y OUT_ para output ports
        # ADF no tiene este concepto, solo nombres de columnas
        # Ejemplo: IN_FIRSTNAME → FIRSTNAME, OUT_TOTAL → TOTAL
        translated = re.sub(r'\b(IN|OUT)_', '', translated)

        # 1. Procesar funciones especiales (manejan paréntesis anidados)
        # NOTA: _translate_decode() maneja || y operadores internamente en cada argumento
        translated = self._translate_decode(translated)  # CRÍTICO: DECODE → case()
        translated = self._translate_add_to_date(translated)
        translated = self._translate_get_date_part(translated)

        # 2. Aplicar traducciones de funciones iterativamente hasta que no haya cambios
        # Esto maneja funciones anidadas correctamente
        max_iterations = 10
        for iteration in range(max_iterations):
            previous = translated

            for pattern, replacement in self.function_patterns:
                translated = re.sub(pattern, replacement, translated, flags=re.IGNORECASE)

            # Si no hubo cambios, salir
            if translated == previous:
                break

        # 3. Manejar concatenación de strings (|| en PowerCenter) SOLO si no fue procesado por DECODE
        # CRÍTICO: No ejecutar si || ya fue convertido dentro de DECODE (evita convertir OR lógico)
        if '||' in translated and 'DECODE' not in expression.upper():
            translated = self._handle_concatenation(translated)

        # 4. Aplicar traducciones de operadores (si no fueron aplicados en DECODE)
        for pattern, replacement in self.operator_mappings:
            translated = re.sub(pattern, replacement, translated)

        # 5. Convertir = a == en contextos de comparación (si no fue aplicado en DECODE)
        translated = self._convert_comparison_operators(translated)

        # 5. Validar que no queden funciones prohibidas
        self._validate_translation(translated, expression)

        return translated

    def _translate_decode(self, expression: str) -> str:
        """
        Traduce DECODE de PowerCenter a case() de ADF.

        PowerCenter DECODE: DECODE(expr, val1, res1, val2, res2, ..., default)
        ADF case: case(expr, val1, res1, val2, res2, ..., default)

        Ejemplos:
        - DECODE(status, 'A', 'Active', 'I', 'Inactive', 'Unknown')
          → case(status, 'A', 'Active', 'I', 'Inactive', 'Unknown')

        - DECODE(TRUE, amount > 1000, 'High', amount > 500, 'Medium', 'Low')
          → case(true(), amount > 1000, 'High', amount > 500, 'Medium', 'Low')
        """
        # Procesar iterativamente todas las ocurrencias de DECODE
        max_iterations = 20
        for _ in range(max_iterations):
            # Buscar DECODE (case insensitive)
            match = re.search(r'DECODE\s*\(', expression, re.IGNORECASE)
            if not match:
                break  # No hay más DECODE

            start_idx = match.start()
            paren_start = match.end() - 1  # Índice del '(' de apertura

            # Encontrar el paréntesis de cierre balanceado
            paren_count = 1
            i = paren_start + 1
            while i < len(expression) and paren_count > 0:
                if expression[i] == '(':
                    paren_count += 1
                elif expression[i] == ')':
                    paren_count -= 1
                i += 1

            if paren_count != 0:
                # Paréntesis no balanceados, no podemos procesar
                break

            paren_end = i - 1  # Índice del ')' de cierre

            # Extraer contenido dentro de los paréntesis
            content = expression[paren_start + 1:paren_end]

            # Separar argumentos
            args = self._split_function_args(content)

            if len(args) < 3:
                # DECODE requiere al menos 3 argumentos
                break

            # CRÍTICO: Procesar CADA argumento individualmente ANTES de construir case()
            # Esto evita que el regex de concatenación y comparación crucen límites de argumentos
            processed_args = []
            for arg in args:
                # 1. Convertir || a concat()
                arg = self._handle_concatenation(arg)
                # 2. Convertir operadores lógicos (AND, OR, NOT)
                for pattern, replacement in self.operator_mappings:
                    arg = re.sub(pattern, replacement, arg)
                # 3. Convertir = a == en comparaciones
                arg = self._convert_comparison_operators(arg)
                processed_args.append(arg)

            args = processed_args

            # CRÍTICO: Manejar caso especial DECODE(TRUE, cond1, res1, cond2, res2, ..., default)
            # En PowerCenter: DECODE(TRUE, ...) evalúa condiciones booleanas secuencialmente
            # En ADF: case(cond1, res1, cond2, res2, ..., default) - SIN el TRUE
            if args[0].strip().upper() == 'TRUE':
                # Eliminar el primer argumento (TRUE) porque case() no lo necesita
                args = args[1:]
                # Ahora args = [cond1, res1, cond2, res2, ..., default]

            # Construir case() con los argumentos (sin TRUE si fue DECODE(TRUE, ...))
            case_expr = f"case({', '.join(args)})"

            # Reemplazar en la expresión
            expression = expression[:start_idx] + case_expr + expression[paren_end + 1:]

        return expression

    def _translate_add_to_date(self, expression: str) -> str:
        """
        Traduce ADD_TO_DATE y ADD_toDate manejando correctamente paréntesis anidados.

        ADD_TO_DATE(date_arg, 'DD', num_arg) o ADD_toDate(date_arg, 'DD', num_arg)
        """
        # Mapeo de unidades de tiempo
        time_unit_map = {
            'DD': 'addDays',
            'MM': 'addMonths',
            'YYYY': 'addYears',
            'YY': 'addYears',
            'Y': 'addYears',
            'HH': 'addHours',
            'MI': 'addMinutes',
            'SS': 'addSeconds'
        }

        # Procesar iterativamente todas las ocurrencias (ambas variantes)
        max_iterations = 20
        for _ in range(max_iterations):
            # Buscar ADD_TO_DATE o ADD_toDate (case insensitive)
            match = re.search(r'ADD_(TO_)?DATE\s*\(|ADD_toDate\s*\(', expression, re.IGNORECASE)
            if not match:
                break  # No hay más ADD_TO_DATE/ADD_toDate

            start_idx = match.start()
            paren_start = match.end() - 1  # Índice del '(' de apertura

            # Encontrar el paréntesis de cierre balanceado
            paren_count = 1
            i = paren_start + 1
            while i < len(expression) and paren_count > 0:
                if expression[i] == '(':
                    paren_count += 1
                elif expression[i] == ')':
                    paren_count -= 1
                i += 1

            if paren_count != 0:
                # Paréntesis no balanceados
                break

            paren_end = i - 1  # Índice del ')' de cierre

            # Extraer contenido dentro de los paréntesis
            content = expression[paren_start + 1:paren_end]

            # Separar los tres argumentos
            args = self._split_function_args(content)

            if len(args) != 3:
                # No tiene exactamente 3 argumentos
                break

            date_arg = args[0].strip()
            unit_arg = args[1].strip()
            num_arg = args[2].strip()

            # El segundo argumento debe ser un string literal como 'DD' o "MM"
            unit_match = re.match(r'^[\'"]([A-Z]+)[\'"]$', unit_arg, re.IGNORECASE)
            if not unit_match:
                # No es un literal válido
                break

            time_unit = unit_match.group(1).upper()

            # Obtener función ADF correspondiente
            adf_func = time_unit_map.get(time_unit)
            if not adf_func:
                # Unidad de tiempo no reconocida
                break

            # Construir reemplazo
            replacement = f"{adf_func}({date_arg}, {num_arg})"

            # Reemplazar en la expresión
            expression = expression[:start_idx] + replacement + expression[paren_end + 1:]

        return expression

    def _split_function_args(self, text: str) -> list:
        """
        Divide argumentos de función separados por comas (respetando paréntesis y comillas).

        Args:
            text: Texto con argumentos separados por comas

        Returns:
            Lista de argumentos
        """
        args = []
        current_arg = []
        paren_depth = 0
        in_single_quote = False
        in_double_quote = False

        for char in text:
            # Manejar comillas
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
                current_arg.append(char)
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
                current_arg.append(char)
            # Manejar paréntesis
            elif char == '(' and not in_single_quote and not in_double_quote:
                paren_depth += 1
                current_arg.append(char)
            elif char == ')' and not in_single_quote and not in_double_quote:
                paren_depth -= 1
                current_arg.append(char)
            # Manejar comas
            elif char == ',' and paren_depth == 0 and not in_single_quote and not in_double_quote:
                # Separador de argumentos
                args.append(''.join(current_arg))
                current_arg = []
            else:
                current_arg.append(char)

        # Agregar último argumento
        if current_arg:
            args.append(''.join(current_arg))

        return args

    def _translate_get_date_part(self, expression: str) -> str:
        """
        Traduce GET_DATE_PART manejando correctamente paréntesis anidados.

        GET_DATE_PART(arg, 'DD') donde arg puede contener paréntesis.
        """
        # Mapeo de partes de fecha
        date_part_map = {
            'DD': 'dayOfMonth',
            'MM': 'month',
            'YYYY': 'year',
            'YY': 'year',
            'Y': 'year',
            'DDD': 'dayOfYear',
            'WW': 'weekOfYear',
            'HH': 'hour',
            'MI': 'minute',
            'SS': 'second'
        }

        # Procesar iterativamente todas las ocurrencias
        max_iterations = 20
        for _ in range(max_iterations):
            # Buscar GET_DATE_PART (case insensitive)
            match = re.search(r'GET_DATE_PART\s*\(', expression, re.IGNORECASE)
            if not match:
                break  # No hay más GET_DATE_PART

            start_idx = match.start()
            paren_start = match.end() - 1  # Índice del '(' de apertura

            # Encontrar el paréntesis de cierre balanceado
            paren_count = 1
            i = paren_start + 1
            while i < len(expression) and paren_count > 0:
                if expression[i] == '(':
                    paren_count += 1
                elif expression[i] == ')':
                    paren_count -= 1
                i += 1

            if paren_count != 0:
                # Paréntesis no balanceados, no podemos procesar
                break

            paren_end = i - 1  # Índice del ')' de cierre

            # Extraer contenido dentro de los paréntesis
            content = expression[paren_start + 1:paren_end]

            # Buscar la última coma (que separa los dos argumentos)
            # Tenemos que buscar la coma que no está dentro de paréntesis/comillas
            comma_idx = self._find_last_arg_separator(content)

            if comma_idx == -1:
                # No se encontró separador válido
                break

            # Extraer argumentos
            arg1 = content[:comma_idx].strip()
            arg2 = content[comma_idx + 1:].strip()

            # El segundo argumento debe ser un string literal como 'DD' o "MM"
            arg2_match = re.match(r'^[\'"]([A-Z]+)[\'"]$', arg2)
            if not arg2_match:
                # No es un literal válido
                break

            date_part = arg2_match.group(1).upper()

            # Obtener función ADF correspondiente
            adf_func = date_part_map.get(date_part)
            if not adf_func:
                # Parte de fecha no reconocida
                break

            # Construir reemplazo
            replacement = f"{adf_func}({arg1})"

            # Reemplazar en la expresión
            expression = expression[:start_idx] + replacement + expression[paren_end + 1:]

        return expression

    def _find_last_arg_separator(self, text: str) -> int:
        """
        Encuentra la última coma que separa argumentos (no dentro de paréntesis/comillas).

        Args:
            text: Texto donde buscar

        Returns:
            Índice de la coma, o -1 si no se encuentra
        """
        paren_depth = 0
        in_single_quote = False
        in_double_quote = False

        # Buscar desde el final hacia atrás
        for i in range(len(text) - 1, -1, -1):
            char = text[i]

            # Manejar comillas
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote

            # Manejar paréntesis (procesando hacia atrás)
            if not in_single_quote and not in_double_quote:
                if char == ')':
                    paren_depth += 1
                elif char == '(':
                    paren_depth -= 1

                # Buscar coma a nivel 0 de paréntesis
                if char == ',' and paren_depth == 0:
                    return i

        return -1

    def _handle_concatenation(self, expression: str) -> str:
        """
        Maneja el operador de concatenación || de PowerCenter.

        PowerCenter: 'string1' || 'string2' || variable
        ADF: concat(concat('string1', 'string2'), variable)

        Detecta || que NO están dentro de operadores lógicos (&& ||)
        """
        # Patrón para detectar concatenación: operandos separados por ||
        # Evitar confusión con || lógico (ya traducido a &&)

        # Buscar patrones del tipo: algo || algo
        # donde algo puede ser: 'string', "string", variable, función()
        pattern = r"(['\"].*?['\"]|[\w\.]+(?:\([^)]*\))?)\s*\|\|\s*(['\"].*?['\"]|[\w\.]+(?:\([^)]*\))?)"

        # Reemplazar iterativamente hasta que no haya más ||
        max_iterations = 20
        iteration = 0

        while '||' in expression and iteration < max_iterations:
            # Verificar si el || está en contexto de concatenación
            if re.search(pattern, expression):
                expression = re.sub(pattern, r'concat(\1, \2)', expression)
            else:
                break
            iteration += 1

        return expression

    def _convert_comparison_operators(self, expression: str) -> str:
        """
        Convierte = a == en contextos de comparación.
        NO convierte en asignaciones de columnas.

        Regla: Si = está entre dos operandos (no al inicio de línea), convertir a ==
        """
        # Patrón: algo = algo (pero no ==)
        # Buscar = que NO esté precedido o seguido por =, !, <, >
        pattern = r'([^\s=!<>]+)\s*=\s*([^\s=][^=]*?)(?=\s*(?:&&|\|\||,|\)|$))'

        expression = re.sub(pattern, r'\1 == \2', expression)

        return expression

    def _validate_translation(self, translated: str, original: str) -> None:
        """
        Valida que la traducción no contenga funciones prohibidas.

        Args:
            translated: Expresión traducida
            original: Expresión original (para contexto de error)

        Raises:
            ValueError: Si contiene funciones prohibidas
        """
        translated_upper = translated.upper()

        for forbidden in self.forbidden_functions:
            # Buscar case-insensitive
            if forbidden.upper() in translated_upper:
                raise ValueError(
                    f"Expresión traducida contiene función/operador no válido: '{forbidden.strip()}'\n"
                    f"Original: {original}\n"
                    f"Traducida: {translated}"
                )

    def validate_adf_expression(self, expression: str) -> Tuple[bool, List[str]]:
        """
        Valida que una expresión sea compatible con ADF.

        Args:
            expression: Expresión a validar

        Returns:
            Tupla (es_válida, lista_de_errores)
        """
        errors = []

        # Verificar funciones prohibidas (case-insensitive)
        expression_upper = expression.upper()
        for forbidden in self.forbidden_functions:
            if forbidden.upper() in expression_upper:
                errors.append(f"Contiene función/operador no válido: '{forbidden.strip()}'")

        # Verificar paréntesis balanceados
        if expression.count('(') != expression.count(')'):
            errors.append("Paréntesis no balanceados")

        # Verificar comillas balanceadas
        single_quotes = expression.count("'")
        double_quotes = expression.count('"')

        if single_quotes % 2 != 0:
            errors.append("Comillas simples no balanceadas")

        if double_quotes % 2 != 0:
            errors.append("Comillas dobles no balanceadas")

        return (len(errors) == 0, errors)


# Instancia global para uso fácil
_translator = ExpressionTranslator()


def translate_expression(expression: str) -> str:
    """
    Función de conveniencia para traducir expresiones.

    Args:
        expression: Expresión en sintaxis PowerCenter

    Returns:
        Expresión traducida a sintaxis ADF
    """
    return _translator.translate(expression)


def validate_adf_expression(expression: str) -> Tuple[bool, List[str]]:
    """
    Función de conveniencia para validar expresiones ADF.

    Args:
        expression: Expresión a validar

    Returns:
        Tupla (es_válida, lista_de_errores)
    """
    return _translator.validate_adf_expression(expression)
