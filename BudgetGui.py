from kivy.uix.gridlayout import GridLayout
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.stacklayout import StackLayout
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView
from kivy.core.window import Window
from kivy.app import App
from kivy.uix.widget import Widget
from kivy.properties import StringProperty
from kivy.lang import Builder
from functools import partial
import importlib
budget_backend = importlib.import_module('Budget')



def change_allocation(instance, arg_alloc_type, top_layout):
    try:
        #update database
        alloc_amount = instance.change_value
        budget_backend.single_allocation(
            category=instance.category,
            year=budget_backend.get_current_year(),
            month=budget_backend.get_current_month(),
            amount=alloc_amount,
            alloc_type=arg_alloc_type
        )

        #get updated data
        if (arg_alloc_type == 'set'):
            top_layout.widget_dict['Alloc_' + instance.category].text = str(alloc_amount)
        elif (arg_alloc_type == 'add'):
            top_layout.widget_dict['Alloc_' + instance.category].text = \
                str(float(top_layout.widget_dict['Alloc_' + instance.category].text) + alloc_amount)

        top_layout.widget_dict['Input_' + instance.category].text = ''

        top_layout.widget_dict['SetBtn_' + instance.category].change_value = 0
        top_layout.widget_dict['AddBtn_' + instance.category].change_value = 0

    except ValueError:
        print(f'{instance.category} cannot be set to {instance.change_value}')



def update_btn_values(instance, value, parent=None):
    #update values that 'Set' and 'Add' button presses will pass to the database
    try:
        parent.widget_dict['SetBtn_' + instance.category].change_value = float(value)
        parent.widget_dict['AddBtn_' + instance.category].change_value = float(value)
    except:
        parent.widget_dict['SetBtn_' + instance.category].change_value = 0
        parent.widget_dict['AddBtn_' + instance.category].change_value = 0



class AllocationButton(Button):
    def __init__(self, category='', change_value=0, **kwargs):
        super().__init__(**kwargs)
        self.category = category
        self.change_value = change_value



class AllocationTextInput(TextInput):
    def __init__(self, category='', **kwargs):
        super().__init__(**kwargs)
        self.category = category



# class BudgetGridLayout(GridLayout):
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.widget_dict = {}



class BudgetApp(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.month = budget_backend.get_current_month()
        self.month_name = budget_backend.get_month_name(self.month)
        self.year = budget_backend.get_current_year()
        self.widget_dict = {}


    def move_time_forward(self):
        self.update_status('Loading...')
        if (self.month < 12):
            self.month += 1
        else:
            self.month = 1
            self.year += 1
        self.month_name = budget_backend.get_month_name(self.month)
        budget_backend.insert_new_year(self.year)
        self.update_time_view()
        self.populate_allocations()
        self.update_status()

    def move_time_backward(self):
        self.update_status('Loading...')
        if (self.month > 1):
            self.month -= 1
        else:
            self.month = 12
            self.year -= 1
        self.month_name = budget_backend.get_month_name(self.month)
        budget_backend.insert_new_year(self.year)
        self.update_time_view()
        self.populate_allocations()
        self.update_status()

    def populate_allocations(self):
        alloc_data = budget_backend.get_allocations_by_period(self.year, self.month)
        trans_data = budget_backend.get_trans_summ_by_period(self.year, self.month)

        for key, widget in self.widget_dict.items():

            alloc_amount = 0
            try:
                alloc_amount = alloc_data.loc[alloc_data['Category'] == key[6:], 'Amount'].iloc[0]
            except IndexError:
                alloc_amount = 0

            trans_amount = 0
            try:
                trans_amount = trans_data.loc[trans_data['Category'] == key[6:], 'SumAmount'].iloc[0]
            except IndexError:
                trans_amount = 0

            if (key[:6] == 'Alloc_'):
                if (alloc_amount < trans_amount):
                    color_tag = '[color=#FF0040]'  # red
                elif (alloc_amount > trans_amount):
                    color_tag = '[color=#F7FE2E]'  # yellow
                else:
                    color_tag = '[color=#FFFFFF]'  # white
                widget.text = color_tag + str(alloc_amount) + '[/color]'

            if (key[:6] == 'Trans_'):
                widget.text = color_tag + str(trans_amount) + '[/color]'


    def update_time_view(self):
        self.widget_dict['Time'].text = self.month_name + ' ' + str(self.year)


    def update_status(self, status=''):
        self.widget_dict['Status'].text = status


    def build(self):
        alloc_data = budget_backend.get_allocations_by_period(self.year, self.month)
        trans_data = budget_backend.get_trans_summ_by_period(self.year, self.month)


        root = ScrollView(size_hint=(1,None), size=(Window.height, Window.width))
        main_grid_layout = GridLayout(cols=7, spacing=10, size_hint_y=None, height=500)
        main_grid_layout.bind(minimum_height=main_grid_layout.setter('height'))
        main_box_layout = BoxLayout(orientation='vertical', size_hint_y=None)
        main_box_layout.bind(minimum_height=main_box_layout.setter('height'))
        top_stack_layout = StackLayout(size_hint_y=None, height=40)
        top_stack_layout.bind(minimum_height=top_stack_layout.setter('height'))

        back_btn = Button(text='Back', size_hint=(.1,None), height=40)
        back_btn.bind(on_press=lambda x: self.move_time_backward())
        top_stack_layout.add_widget(back_btn)
        self.widget_dict['Time'] = Label(text='[b]' + self.month_name + ' ' + str(self.year) + '[/b]', size_hint=(.1,None), height=40, markup=True)
        top_stack_layout.add_widget(self.widget_dict['Time'])
        forward_btn = Button(text='Next', size_hint=(.1,None), height=40)
        forward_btn.bind(on_press=lambda x: self.move_time_forward())
        top_stack_layout.add_widget(forward_btn)
        self.widget_dict['Status'] = Label(text='', size_hint=(0.15,None), height=40)
        top_stack_layout.add_widget(self.widget_dict['Status'])
        # status_bar = FloatLayout()
        # self.widget_dict['Status'] = Label(text='TEST', size_hint=(None,None), height=40, pos_hint={'right':1, 'center_y':0.5})
        # status_bar.add_widget(self.widget_dict['Status'])
        # top_stack_layout.add_widget(status_bar)

        main_grid_layout.add_widget(Label(text='[b]Section[/b]', size_hint_y=None, height=80, font_size='20sp', markup=True))
        main_grid_layout.add_widget(Label(text='[b]Category[/b]', size_hint_y=None, height=80, font_size='20sp', markup=True))
        main_grid_layout.add_widget(Label(text='[b]Allocated[/b]', size_hint_y=None, height=80, font_size='20sp', markup=True))
        main_grid_layout.add_widget(Label(text='[b]Spent[/b]', size_hint_y=None, height=80, font_size='20sp', markup=True))
        main_grid_layout.add_widget(Label(text='', size_hint_y=None, height=80, font_size='20sp', markup=True))
        main_grid_layout.add_widget(Label(text='', size_hint_y=None, height=80, font_size='20sp', markup=True))
        main_grid_layout.add_widget(Label(text='', size_hint_y=None, height=80, font_size='20sp', markup=True))

        for row in alloc_data.itertuples():
            try:
                sum_trans_amt = trans_data.loc[trans_data['Category'] == row.Category, 'SumAmount'].iloc[0]
            except IndexError:
                sum_trans_amt = 0

            if (row.Amount < sum_trans_amt):
                color_tag = '[color=#FF0040]' #red
            elif (row.Amount > sum_trans_amt):
                color_tag = '[color=#F7FE2E]' #yellow
            else:
                color_tag = '[color=#FFFFFF]' #white

            self.widget_dict['Section_' + row.Category] = Label(text=row.Section, size_hint_y=None, height=40)
            main_grid_layout.add_widget(self.widget_dict['Section_' + row.Category])

            self.widget_dict['Label_' + row.Category] = Label(text=row.Category, size_hint_y=None, height=40)
            main_grid_layout.add_widget(self.widget_dict['Label_' + row.Category])


            self.widget_dict['Alloc_' + row.Category] = Label(text=color_tag + str(row.Amount) + '[/color]',
                                                              size_hint_y=None, height=40, markup=True)
            main_grid_layout.add_widget(self.widget_dict['Alloc_' + row.Category])

            self.widget_dict['Trans_' + row.Category] = Label(text=color_tag + str(sum_trans_amt) + '[/color]',
                                                              size_hint_y=None, height=40, markup=True)
            main_grid_layout.add_widget(self.widget_dict['Trans_' + row.Category])

            self.widget_dict['Input_' + row.Category] = AllocationTextInput(
                size_hint_y=None, height=40, multiline=False, category=row.Category)
            self.widget_dict['Input_' + row.Category].bind(text=partial(update_btn_values, parent=self))
            main_grid_layout.add_widget(self.widget_dict['Input_' + row.Category])

            self.widget_dict['SetBtn_' + row.Category] = AllocationButton(
                text='Set', size_hint_y=None, height=40, category=row.Category)
            self.widget_dict['SetBtn_' + row.Category].bind(
                on_press=partial(change_allocation, arg_alloc_type='set', top_layout=self))
            main_grid_layout.add_widget(self.widget_dict['SetBtn_' + row.Category])

            self.widget_dict['AddBtn_' + row.Category] = AllocationButton(
                text='Add', size_hint_y=None, height=40, category=row.Category)
            self.widget_dict['AddBtn_' + row.Category].bind(
                on_press=partial(change_allocation, arg_alloc_type='add', top_layout=self))
            main_grid_layout.add_widget(self.widget_dict['AddBtn_' + row.Category])


        main_box_layout.add_widget(top_stack_layout)
        main_box_layout.add_widget(main_grid_layout)
        root.add_widget(main_box_layout)
        return root

if __name__ == '__main__':
    BudgetApp().run()
