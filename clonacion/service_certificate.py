import base64
from io import BytesIO
import qrcode
from utils import create_pdf,upload_pdf
import mysql.connector


class ServiceCertificatePDF:
    @staticmethod
    def build(id,pk):
        conn = mysql.connector.connect(
            host="164.92.98.250",
            user="Workbench",
            password="password20@@",
            database="pwa"
            )
        cursor = conn.cursor()
   
        order_query = """
            SELECT 
                so.id as folio, so.id_service_order, so.created_at as date, e.initial_hour,
                e.initial_date, em.name, em.file_route_firm, c.name as cliente, c.establishment_name as empresa,
                c.cellphone, cd.address, c.municipality, q.id_plague_jer, pt.plague_key,
                u.name as agente, st.name as status, cd.address_number, cd.state, cd.email, e.id_status,
                e.id as event, c.colony, q.id as quotation, e.final_hour, e.final_date, so.address as a_rfc,
                so.email as e_rfc, so.bussiness_name, so.observations, cd.billing, d.id as discount,
                d.percentage, ex.id as extra, ex.amount, q.construction_measure, q.garden_measure, q.price,
                q.establishment_id as e_id, pm.id as pm_id, pw.id as pw_id, pm.name as metodo, pw.name as tipo,
                so.id_job_center, cd.email, q.companie as compania, e.start_event, e.final_event, pl.show_price,
                et.name as establecimiento, so.customer_branch_id, so.total, cd.customer_id,
                pl.portada as pdf_mip, pjc.sanitary_license, pl.days_expiration_certificate, c.show_price as show_price_customer,
                so.date_expiration_certificate, so.area_node_id, c.days_expiration_certificate as days_expiration_certificate_customer,
                pl.is_disinfection, so.is_main, so.is_shared, em.id as employeeId
            FROM events as e
            JOIN employees as em ON e.id_employee = em.id
            JOIN profile_job_centers as pjc ON e.id_job_center = pjc.id
            JOIN service_orders as so ON e.id_service_order = so.id
            JOIN payment_methods as pm ON so.id_payment_method = pm.id
            JOIN payment_ways as pw ON so.id_payment_way = pw.id
            JOIN users as u ON so.user_id = u.id
            JOIN statuses as st ON so.id_status = st.id
            JOIN quotations as q ON so.id_quotation = q.id
            JOIN establishment_types as et ON q.establishment_id = et.id
            JOIN customers as c ON q.id_customer = c.id
            JOIN customer_datas as cd ON cd.customer_id = c.id
            JOIN plague_types as pt ON q.id_plague_jer = pt.id
            JOIN discount_quotation as qd ON q.id = qd.quotation_id
            JOIN discounts as d ON qd.discount_id = d.id
            JOIN extras as ex ON q.id_extra = ex.id
            JOIN price_lists as pl ON q.id_price_list = pl.id
            WHERE so.id = %s
        """
        cursor.execute(order_query, (id,))
        order = cursor.fetchone()
        order_dict = dict(zip([desc[0] for desc in cursor.description], order)) if order else None
        
        company_query = """
            SELECT pdf_logo, pdf_sello, phone, licence, facebook, warnings_service,
                contract_service, pdf_sanitary_license, rfc, bussines_name, health_manager, email
            FROM companies
            WHERE id = %s
        """
        cursor.execute(company_query, (order_dict['compania'],))
        imagen = cursor.fetchone()
        imagen_dict = dict(zip([desc[0] for desc in cursor.description], imagen)) if imagen else None
        
        qrcode_data = None
        if order_dict and order_dict.get('sanitary_license'):
            sanitary_license_url = f"https://pwa-public.s3.us-west-1.amazonaws.com/{order_dict['sanitary_license']}"
            qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_H, box_size=10, border=4)
            qr.add_data(sanitary_license_url)
            qr.make(fit=True)
            img = qr.make_image(fill_color="black", back_color="white")
            
            buffered = BytesIO()
            img.save(buffered, format="PNG")
            qrcode_data = base64.b64encode(buffered.getvalue()).decode()
    
        pdf_logo =imagen_dict.get('pdf_logo')
        pdf_sello = imagen_dict.get('pdf_sello')
        
        auxiliaries_query = """
            SELECT e.name, shares_service_order.id_service_order
            FROM shares_service_order
            JOIN employees as e ON shares_service_order.id_employee = e.id
            JOIN service_orders as so ON shares_service_order.id_service_order = so.id
            JOIN events as ev ON ev.id_service_order = so.id
            WHERE id_service_order_main = %s
            AND shares_service_order.id_employee <> %s
            AND ev.id_status = 4
        """
        cursor.execute(auxiliaries_query, (id, order_dict['employeeId']))
        employee_auxiliaries = cursor.fetchall()
        employee_auxiliaries_list = [dict(zip(['name', 'id_service_order'], row)) for row in employee_auxiliaries]
        
        plagues = []
        place_inspections_merge = []
        order_cleaning_place_conditions = []
        place_conditions_merge = []
        plague_controls = []
        application_methods = []
        plague_controls_products = []
        
        if employee_auxiliaries_list:
            for aux in employee_auxiliaries_list:
                plague_query = """
                    SELECT pl.name as plaga, ie.name as infestacion
                    FROM place_inspection_plague_type as pt
                    JOIN place_inspections as pi ON pt.place_inspection_id = pi.id
                    JOIN plague_types as pl ON pt.plague_type_id = pl.id
                    JOIN infestation_degrees as ie ON pt.id_infestation_degree = ie.id
                    WHERE pi.id_service_order = %s
                """
                cursor.execute(plague_query, (aux['id_service_order'],))
                plagues.extend([dict(zip(['plaga', 'infestacion'], row)) for row in cursor.fetchall()])
                
                # Inspecciones del lugar
                place_inspection_query = "SELECT * FROM place_inspections WHERE id_service_order = %s"
                cursor.execute(place_inspection_query, (aux['id_service_order'],))
                place_inspection = cursor.fetchone()
                cursor.fetchall()
                if place_inspection:
                    place_inspections_merge.append(dict(zip([desc[0] for desc in cursor.description], place_inspection)))
                
                # Orden y Limpieza
                order_cleaning_query = """
                    SELECT co.place_condition_id, o.name as folio
                    FROM order_cleaning_place_condition as co
                    JOIN place_conditions as pc ON co.place_condition_id = pc.id
                    JOIN order_cleanings as o ON co.order_cleaning_id = o.id
                    WHERE pc.id_service_order = %s
                """
                cursor.execute(order_cleaning_query, (aux['id_service_order'],))
                order_cleaning_place_conditions.extend([dict(zip(['place_condition_id', 'folio'], row)) for row in cursor.fetchall()])
                
                # Condiciones del lugar
                place_condition_query = "SELECT restricted_access FROM place_conditions WHERE id_service_order = %s"
                cursor.execute(place_condition_query, (aux['id_service_order'],))
                place_condition = cursor.fetchone()
                cursor.fetchall()
                if place_condition:
                    place_conditions_merge.append(dict(zip(['restricted_access'], place_condition)))
                
                # Control de plagas
                plague_control_query = """
                    SELECT pc.id_service_order, pc.control_areas, pc.commentary, em.name
                    FROM plague_controls as pc
                    JOIN service_orders as so ON pc.id_service_order = so.id
                    JOIN events as e ON e.id_service_order = so.id
                    JOIN employees as em ON e.id_employee = em.id
                    WHERE pc.id_service_order = %s
                """
                cursor.execute(plague_control_query, (aux['id_service_order'],))
                plague_controls.extend([dict(zip(['id_service_order', 'control_areas', 'commentary', 'name'], row)) for row in cursor.fetchall()])
                
                # Métodos de aplicación
                application_method_query = """
                    SELECT pc.id_service_order, pc.control_areas, am.name as apli
                    FROM plague_controls as pc
                    JOIN plague_controls_application_methods as pca ON pc.id = pca.plague_control_id
                    JOIN application_methods as am ON pca.id_application_method = am.id
                    WHERE pc.id_service_order = %s
                """
                cursor.execute(application_method_query, (aux['id_service_order'],))
                application_methods.extend([dict(zip(['id_service_order', 'control_areas', 'apli'], row)) for row in cursor.fetchall()])
                
                # Productos para control de plagas
                plague_products_query = """
                    SELECT 
                        pl.id_service_order, p.name as product, pc.dose, pc.quantity, p.id as id_product, 
                        pl.control_areas, p.active_ingredient, p.register, pc.quantity as cantidad, 
                        em.name as name_employee, pu.name as type_unit
                    FROM plague_controls_products as pc
                    JOIN plague_controls as pl ON pc.plague_control_id = pl.id
                    JOIN service_orders as so ON pl.id_service_order = so.id
                    JOIN events as e ON e.id_service_order = so.id
                    JOIN employees as em ON e.id_employee = em.id
                    JOIN products as p ON pc.id_product = p.id
                    JOIN product_units as pu ON p.id_unit = pu.id
                    WHERE pl.id_service_order = %s
                """
                cursor.execute(plague_products_query, (aux['id_service_order'],))
                plague_controls_products.extend([dict(zip([
                    'id_service_order', 'product', 'dose', 'quantity', 'id_product', 'control_areas', 
                    'active_ingredient', 'register', 'cantidad', 'name_employee', 'type_unit'
                ], row)) for row in cursor.fetchall()])
        
        # Grado de Infestación de Plagas
        main_plague_query = """
            SELECT pl.name as plaga, ie.name as infestacion
            FROM place_inspection_plague_type as pt
            JOIN place_inspections as pi ON pt.place_inspection_id = pi.id
            JOIN plague_types as pl ON pt.plague_type_id = pl.id
            JOIN infestation_degrees as ie ON pt.id_infestation_degree = ie.id
            WHERE pi.id_service_order = %s
        """
        cursor.execute(main_plague_query, (id,))
        main_plagues = [dict(zip(['plaga', 'infestacion'], row)) for row in cursor.fetchall()]
        plagues_merge = plagues + main_plagues if employee_auxiliaries_list else main_plagues
        main_order_cleaning_query = """
            SELECT co.place_condition_id, o.name as name
            FROM order_cleaning_place_condition as co
            JOIN place_conditions as pc ON co.place_condition_id = pc.id
            JOIN order_cleanings as o ON co.order_cleaning_id = o.id
            WHERE pc.id_service_order = %s
        """
        cursor.execute(main_order_cleaning_query, (id,))
        main_order_cleaning = [dict(zip(['place_condition_id', 'folio'], row)) for row in cursor.fetchall()]
        
        order_cleaning_merge = order_cleaning_place_conditions + main_order_cleaning if employee_auxiliaries_list else main_order_cleaning
        main_place_inspection_query = "SELECT * FROM place_inspections WHERE id_service_order = %s"
        cursor.execute(main_place_inspection_query, (id,))
        main_place_inspection = cursor.fetchone()
        cursor.fetchall()
        main_place_inspection_dict = dict(zip([desc[0] for desc in cursor.description], main_place_inspection)) if main_place_inspection else None
        
        if main_place_inspection_dict:
            place_inspections_merge.append(main_place_inspection_dict) if employee_auxiliaries_list else place_inspections_merge.append(main_place_inspection_dict)
        main_plague_products_query = """
            SELECT 
                pl.id_service_order, p.name as product, pc.dose, pc.quantity, p.id as id_product, 
                pl.control_areas, p.active_ingredient, p.register, pc.quantity as cantidad, 
                em.name as name_employee, pu.name as type_unit
            FROM plague_controls_products as pc
            JOIN plague_controls as pl ON pc.plague_control_id = pl.id
            JOIN service_orders as so ON pl.id_service_order = so.id
            JOIN events as e ON e.id_service_order = so.id
            JOIN employees as em ON e.id_employee = em.id
            JOIN products as p ON pc.id_product = p.id
            JOIN product_units as pu ON p.id_unit = pu.id
            WHERE pl.id_service_order = %s
        """
        cursor.execute(main_plague_products_query, (id,))
        main_plague_products = [dict(zip([
            'id_service_order', 'product', 'dose', 'quantity', 'id_product', 'control_areas', 
            'active_ingredient', 'register', 'cantidad', 'name_employee', 'type_unit'
        ], row)) for row in cursor.fetchall()]
        
        plague_products_merge = plague_controls_products + main_plague_products if employee_auxiliaries_list else main_plague_products
        
        # Control de plagas
        main_plague_control_query = """
            SELECT pc.id_service_order, pc.control_areas, pc.commentary, em.name
            FROM plague_controls as pc
            JOIN service_orders as so ON pc.id_service_order = so.id
            JOIN events as e ON e.id_service_order = so.id
            JOIN employees as em ON e.id_employee = em.id
            WHERE pc.id_service_order = %s
        """
        cursor.execute(main_plague_control_query, (id,))
        main_plague_control = [dict(zip(['id_service_order', 'control_areas', 'commentary', 'name'], row)) for row in cursor.fetchall()]
        
        plague_control_merge = plague_controls + main_plague_control if employee_auxiliaries_list else main_plague_control
        
        # Métodos de aplicación
        main_application_method_query = """
            SELECT pc.id_service_order, pc.control_areas, am.name as apli
            FROM plague_controls as pc
            JOIN plague_controls_application_methods as pca ON pc.id = pca.plague_control_id
            JOIN application_methods as am ON pca.id_application_method = am.id
            WHERE pc.id_service_order = %s
        """
        cursor.execute(main_application_method_query, (id,))
        main_application_methods = [dict(zip(['id_service_order', 'control_areas', 'apli'], row)) for row in cursor.fetchall()]
        
        application_methods_merge = application_methods + main_application_methods if employee_auxiliaries_list else main_application_methods
        
        # Condiciones del lugar
        main_place_condition_query = "SELECT restricted_access FROM place_conditions WHERE id_service_order = %s"
        cursor.execute(main_place_condition_query, (id,))
        main_place_condition = cursor.fetchone()
        cursor.fetchall()
        main_place_condition_dict = dict(zip(['restricted_access'], main_place_condition)) if main_place_condition else None
        
        if main_place_condition_dict:
            place_conditions_merge.append(main_place_condition_dict) if employee_auxiliaries_list else place_conditions_merge.append(main_place_condition_dict)
        
        # Indicaciones
        indications_query = "SELECT indications FROM place_conditions WHERE id_service_order = %s"
        cursor.execute(indications_query, (id,))
        indications = cursor.fetchone()
        cursor.fetchall()
        indications_dict = dict(zip(['indications'], indications)) if indications else None
        
        # Firma del servicio
        firm_query = "SELECT file_route, other_name FROM service_firms WHERE id_service_order = %s"
        cursor.execute(firm_query, (id,))
        firm = cursor.fetchone()
        cursor.fetchall()
        firm_dict = dict(zip(['file_route', 'other_name'], firm)) if firm else None
        
        if firm_dict:
            firm_url = firm_dict['file_route']
        else:
            firm_url = None 
        
        firm_url_technician = order_dict['file_route_firm']
        
        if order_dict and order_dict.get('customer_branch_id'):
            customer_branch_query = "SELECT * FROM customer_branches WHERE id = %s"
            cursor.execute(customer_branch_query, (order_dict['customer_branch_id'],))
            customer_branch = cursor.fetchone()
            cursor.fetchall()
            customer_branch_dict = dict(zip([desc[0] for desc in cursor.description], customer_branch)) if customer_branch else None
            
            if customer_branch_dict:
                order_dict['total'] = order_dict['total']
                order_dict['empresa'] = customer_branch_dict['name']
                order_dict['address'] = customer_branch_dict['address']
                order_dict['address_number'] = customer_branch_dict['address_number']
                order_dict['colony'] = customer_branch_dict['colony']
                order_dict['municipality'] = customer_branch_dict['municipality']
                order_dict['state'] = customer_branch_dict['state']
        
        job_center_query = "SELECT * FROM profile_job_centers WHERE id = %s"
        cursor.execute(job_center_query, (order_dict['id_job_center'],))
        job_center = cursor.fetchone()
        cursor.fetchall()
        job_center_dict = dict(zip([desc[0] for desc in cursor.description], job_center)) if job_center else None
        
        address_profile_query = """
            SELECT * FROM address_job_centers 
            WHERE profile_job_centers_id = %s
        """
        cursor.execute(address_profile_query, (job_center_dict['profile_job_centers_id'],))
        address_profile = cursor.fetchone()
        cursor.fetchall()
        address_profile_dict = dict(zip([desc[0] for desc in cursor.description], address_profile)) if address_profile else None
        
        text_node = ""
        if order_dict and order_dict.get('area_node_id'):
            area_tree_query = "SELECT text FROM area_trees WHERE id = %s"
            cursor.execute(area_tree_query, (order_dict['area_node_id'],))
            area_tree = cursor.fetchone()
            cursor.fetchall()

            if area_tree:
                text_node = area_tree[0]
        
        if order_dict:
            order_dict['textNode'] = text_node
            order_dict['customTax'] = order_dict['total'] * 0.07
            order_dict['customTotal'] = order_dict['customTax'] + order_dict['total']
        cursor.close()
        conn.close()
        
        cont = {
            'order': order_dict,
            'jobCenterProfile': job_center_dict,
            'ins': order_cleaning_merge,
            'plaga': plagues_merge,
            'control': plague_products_merge,
            'f': firm_dict,
            'area_c': plague_control_merge,
            'ani': place_inspections_merge,
            'indi': indications_dict,
            'placeConditionsMerge': place_conditions_merge,
            'area_cd': application_methods_merge,
            'imagen': imagen_dict,
            'qrcode': qrcode_data,
            'addressProfile': address_profile_dict,
            'symbol_country':'$',
            'firmUrl': firm_url,
            'firmUrlTechnician': firm_url_technician,
            'employeeAuxiliares': employee_auxiliaries_list,
            'pdf_logo': pdf_logo,
            'pdf_sello': pdf_sello
        }
        pdf = create_pdf(context=cont,template_name='service_certificate.html' )
        upload_pdf (pdf, key  =f'events/certificates/{pk}.pdf')
        
        